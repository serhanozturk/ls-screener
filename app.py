"""
L/S DIVERGENCE SCREENER (v1)
=============================
Binance futures'taki TUM USDT coinleri tarar; account(kalabalik) vs
position(para) ayrismasi yasayanlari kademeli olarak listeler.

- Kapsam: tum USDT futures (~400), exchangeInfo'dan otomatik
- Periyot: 15m + 1h (ikisi de taranir, ayri cache)
- Cron: 30dk'da bir /api/run-scan tetikler
- weight=0 endpoint'ler (account + position L/S), 1000/5dk limit
- Filtre: |ayrisma| >= 5 (UYUMLU gizli)
- Ban tracker + retry (v4.1 altyapisi)

Calistirma: python3 app.py
"""

import http.server
import socketserver
import urllib.request
import urllib.parse
import urllib.error
import json
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

PORT = int(os.environ.get("PORT", 8766))
HOST = "0.0.0.0"
USER_AGENT = "Mozilla/5.0 LSScreener/1.0"

# ===== Binance yerel ban takibi =====
_ban_until = 0.0
_BAN_DEFAULT_MAX = 1800
_BAN_HEADER_MAX = 86400

def _binance_banned():
    return time.time() < _ban_until

def _set_binance_ban(secs, from_header=False):
    global _ban_until
    cap = _BAN_HEADER_MAX if from_header else _BAN_DEFAULT_MAX
    secs = min(max(int(secs), 10), cap)
    until = time.time() + secs
    if until > _ban_until:
        _ban_until = until


def http_get(url, timeout=10, retries=1):
    is_binance = "fapi.binance.com" in url
    if is_binance and _binance_banned():
        raise RuntimeError("Binance gecici banli (yerel takip)")
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if is_binance and e.code in (418, 429):
                ra = 0
                try:
                    ra = int(e.headers.get("Retry-After") or 0)
                except Exception:
                    pass
                if ra > 0:
                    _set_binance_ban(ra, from_header=True)
                else:
                    _set_binance_ban(300 if e.code == 418 else 60)
                raise
            if e.code >= 500 and attempt < retries:
                time.sleep(0.5); continue
            raise
        except Exception:
            if attempt < retries:
                time.sleep(0.5); continue
            raise


# ===== Tarama durumu (her periyot icin ayri cache) =====
_scan_lock = threading.Lock()
_scan_state = {
    "15m": {"ts": 0, "results": [], "scanning": False, "scanned": 0, "total": 0, "error": None},
    "1h":  {"ts": 0, "results": [], "scanning": False, "scanned": 0, "total": 0, "error": None},
}


def get_usdt_symbols():
    """Tum aktif USDT perpetual futures sembolleri (exchangeInfo)."""
    data = http_get("https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=15)
    syms = []
    for s in data.get("symbols", []):
        if (s.get("quoteAsset") == "USDT"
                and s.get("contractType") == "PERPETUAL"
                and s.get("status") == "TRADING"):
            syms.append(s["symbol"])
    return syms


def _fetch_pair(sym, period):
    """Bir coin icin account + position + funding. Donus dict veya None."""
    acc = pos = funding = None
    # Account ratio (kalabalik)
    try:
        j = http_get(f"https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol={sym}&period={period}&limit=1")
        if isinstance(j, list) and j:
            acc = float(j[-1]["longAccount"]) * 100
    except Exception:
        pass
    # Position ratio (para - top trader)
    try:
        j = http_get(f"https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol={sym}&period={period}&limit=1")
        if isinstance(j, list) and j:
            pos = float(j[-1]["longAccount"]) * 100
    except Exception:
        pass
    if acc is None or pos is None:
        return None
    # Funding (premiumIndex - tek cagri, weight dusuk)
    try:
        pj = http_get(f"https://fapi.binance.com/fapi/v1/premiumIndex?symbol={sym}")
        fr = pj.get("lastFundingRate")
        if fr not in (None, ""):
            funding = float(fr) * 100
    except Exception:
        pass

    diff = pos - acc  # pozitif = para daha long = whale long / retail short
    return {
        "symbol": sym.replace("USDT", ""),
        "account": round(acc, 2),
        "position": round(pos, 2),
        "divergence": round(diff, 2),
        "funding": round(funding, 4) if funding is not None else None,
    }


def _label(diff):
    a = abs(diff)
    if a >= 15: level = "GUCLU"
    elif a >= 10: level = "ORTA"
    elif a >= 5: level = "HAFIF"
    else: level = "UYUMLU"
    direction = "WHALE LONG / RETAIL SHORT" if diff > 0 else "WHALE SHORT / RETAIL LONG"
    return level, direction


def _funding_conflict(diff, funding):
    """Funding ile ayrisma celiskisi (asil aranan sinyal):
    - funding+ (longlar oduyor) ama whale SHORT -> para asagi bahse ragmen kalabalik long
    - funding- (shortlar oduyor) ama whale LONG  -> para yukari bahse ragmen kalabalik short
    Celiski = para (position) yonu ile funding yonu TERS."""
    if funding is None or abs(diff) < 5:
        return False
    # whale long (diff>0) + funding negatif = celiski; whale short (diff<0) + funding pozitif = celiski
    if diff > 0 and funding < -0.005:
        return True
    if diff < 0 and funding > 0.005:
        return True
    return False


def run_scan(period):
    """Tum coinleri tara, ayrisanlari (>=5) doldur. Thread-safe state gunceller."""
    st = _scan_state[period]
    with _scan_lock:
        if st["scanning"]:
            return {"ok": True, "already": True}
        st["scanning"] = True
        st["error"] = None
        st["scanned"] = 0

    try:
        syms = get_usdt_symbols()
        st["total"] = len(syms)
        results = []
        # Gruplar halinde + hafif throttle (weight=0 ama yine de nazik)
        BATCH = 15
        for i in range(0, len(syms), BATCH):
            if _binance_banned():
                st["error"] = "Binance banli, tarama yarida kesildi"
                break
            batch = syms[i:i+BATCH]
            with ThreadPoolExecutor(max_workers=BATCH) as ex:
                futs = {ex.submit(_fetch_pair, s, period): s for s in batch}
                for f in as_completed(futs):
                    r = f.result()
                    if r and abs(r["divergence"]) >= 5:
                        results.append(r)
            st["scanned"] = min(i + BATCH, len(syms))
            time.sleep(0.4)  # gruplar arasi nefes
        # Ayrisma buyuklugune gore sirala (mutlak deger, buyukten kucuge)
        results.sort(key=lambda r: abs(r["divergence"]), reverse=True)
        for r in results:
            lvl, direction = _label(r["divergence"])
            r["level"] = lvl
            r["direction"] = direction
            r["fundingConflict"] = _funding_conflict(r["divergence"], r["funding"])
        with _scan_lock:
            st["results"] = results
            st["ts"] = time.time()
        return {"ok": True, "count": len(results)}
    except Exception as e:
        st["error"] = str(e)
        return {"ok": False, "error": str(e)}
    finally:
        st["scanning"] = False


SCREENER_HTML = '''<!DOCTYPE html>
<html lang="tr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover">
<meta name="theme-color" content="#0a0e0d">
<title>L/S Divergence Screener</title>
<link rel="manifest" href="/manifest.json">
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;700&family=Major+Mono+Display&display=swap" rel="stylesheet">
<style>
:root {
--bg:#0a0e0d; --bg-2:#0f1413; --border:#1f2a28; --border-strong:#2a3a37;
--text:#d4dcd9; --text-dim:#6e7976; --text-faint:#3f4845;
--green:#00d09c; --red:#ff4d6d; --red-dim:#a82d44; --accent:#6df5d4; --amber:#ffb83d;
}
body.light {
--bg:#f4f6f5; --bg-2:#ffffff; --border:#dde3e1; --border-strong:#c4cecb;
--text:#1a2422; --text-dim:#6e7976; --text-faint:#a8b2af;
--green:#00a37a; --red:#e02e4d; --accent:#00a37a; --amber:#d68a00; --red-dim:#f0c0c8;
}
* { box-sizing:border-box; margin:0; padding:0; -webkit-tap-highlight-color:transparent; }
html,body { background:var(--bg); color:var(--text); font-family:'JetBrains Mono',monospace;
font-size:13px; line-height:1.5; min-height:100vh; -webkit-font-smoothing:antialiased; transition:background 0.3s,color 0.3s; }
.wrap { max-width:1100px; margin:0 auto; padding:20px;
padding-top:calc(20px + env(safe-area-inset-top)); padding-bottom:calc(20px + env(safe-area-inset-bottom)); }
header { display:flex; align-items:center; justify-content:space-between;
padding-bottom:16px; border-bottom:1px solid var(--border); margin-bottom:18px; gap:12px; flex-wrap:wrap; }
.logo { font-family:'Major Mono Display',monospace; font-size:18px; letter-spacing:0.04em; }
.logo span { color:var(--green); }
.meta { display:flex; gap:12px; align-items:center; font-size:11px; color:var(--text-dim); }
.clocks { display:flex; flex-direction:column; gap:1px; text-align:right; }
.theme-btn { background:transparent; border:1px solid var(--border-strong); color:var(--text);
font-size:15px; width:34px; height:34px; cursor:pointer; border-radius:0; }
.theme-btn:hover { border-color:var(--text-dim); }
.controls { display:flex; gap:10px; align-items:center; margin-bottom:16px; flex-wrap:wrap; }
.tabs { display:flex; gap:4px; }
.tabs button { background:transparent; border:1px solid var(--border); color:var(--text-dim);
font-family:inherit; font-size:12px; padding:8px 18px; cursor:pointer; letter-spacing:0.08em; border-radius:0; }
.tabs button.active { background:var(--green); color:var(--bg); border-color:var(--green); font-weight:700; }
.refresh-btn { background:transparent; border:1px solid var(--border-strong); color:var(--text);
font-family:inherit; font-size:11px; padding:8px 14px; cursor:pointer; letter-spacing:0.08em; border-radius:0; margin-left:auto; }
.refresh-btn:hover { border-color:var(--green); color:var(--green); }
.status { font-size:11px; color:var(--text-dim); margin-bottom:14px; min-height:16px; }
.status .scanning { color:var(--amber); }
.status .err { color:var(--red); }
.legend { display:flex; gap:14px; flex-wrap:wrap; margin-bottom:14px; font-size:10px; color:var(--text-dim); }
.legend span { display:flex; align-items:center; gap:5px; }
.dot { width:8px; height:8px; border-radius:50%; display:inline-block; }
.dot.guclu { background:var(--red); box-shadow:0 0 6px var(--red); }
.dot.orta { background:var(--amber); }
.dot.hafif { background:var(--text-dim); }
.dot.conflict { background:var(--accent); box-shadow:0 0 6px var(--accent); }
table { width:100%; border-collapse:collapse; font-size:12px; }
thead th { text-align:right; padding:8px 10px; color:var(--text-dim); font-size:10px;
letter-spacing:0.1em; text-transform:uppercase; border-bottom:1px solid var(--border); font-weight:500; }
thead th.l { text-align:left; }
tbody td { padding:9px 10px; text-align:right; border-bottom:1px solid var(--border); }
tbody td.l { text-align:left; }
tbody tr:hover { background:var(--bg-2); }
.coin { font-weight:700; font-size:13px; letter-spacing:0.03em; }
.acc { color:var(--text-dim); }
.pos { color:var(--amber); }
.div-pos { color:var(--green); font-weight:700; }
.div-neg { color:var(--red); font-weight:700; }
.fund-pos { color:var(--green); }
.fund-neg { color:var(--red); }
.tag { display:inline-block; font-size:9px; padding:2px 7px; border:1px solid; letter-spacing:0.05em; white-space:nowrap; }
.tag.whale-long { color:var(--green); border-color:var(--green); }
.tag.whale-short { color:var(--red); border-color:var(--red); }
.tag-level { font-size:9px; color:var(--text-faint); margin-left:5px; }
.conflict-badge { display:inline-block; font-size:9px; padding:2px 6px; margin-left:6px;
background:var(--accent); color:var(--bg); font-weight:700; letter-spacing:0.03em; }
.section-row td { background:var(--bg-2); color:var(--text-dim); font-size:10px;
letter-spacing:0.15em; text-transform:uppercase; padding:7px 10px; font-weight:700; border-bottom:1px solid var(--border-strong); }
.empty { text-align:center; color:var(--text-faint); padding:40px 0; font-size:12px; }
.spin { display:inline-block; animation:spin 1s linear infinite; }
@keyframes spin { to { transform:rotate(360deg); } }
.info { margin-top:24px; padding:14px; background:var(--bg-2); border:1px dashed var(--border-strong);
font-size:11px; color:var(--text-dim); line-height:1.7; }
.info b { color:var(--text); }
@media (max-width:680px) {
.wrap { padding:14px; }
thead th.hide-m, tbody td.hide-m { display:none; }
.logo { font-size:15px; }
tbody td { padding:8px 6px; font-size:11px; }
thead th { padding:7px 6px; }
.tabs button { padding:8px 14px; }
}
</style>
</head>
<body>
<div class="wrap">
<header>
<div class="logo">L/S<span>&middot;</span>SCREENER</div>
<div class="meta">
<div class="clocks">
<span id="clockTR">--.-- --:--:-- TR</span>
<span id="clockUTC">--.-- --:--:-- UTC</span>
</div>
<button class="theme-btn" id="themeBtn" title="Tema">&#9789;</button>
</div>
</header>

<div class="controls">
<div class="tabs" id="tabs">
<button data-period="15m">15M</button>
<button data-period="1h" class="active">1H</button>
</div>
<button class="refresh-btn" id="refreshBtn">&#8635; YENILE</button>
</div>

<div class="status" id="status">Yukleniyor...</div>

<div class="legend">
<span><span class="dot guclu"></span>GUCLU (15+)</span>
<span><span class="dot orta"></span>ORTA (10-15)</span>
<span><span class="dot hafif"></span>HAFIF (5-10)</span>
<span><span class="dot conflict"></span>FUNDING CELISKISI</span>
</div>

<table>
<thead>
<tr>
<th class="l">COIN</th>
<th class="hide-m">HESAP</th>
<th class="hide-m">POZISYON</th>
<th>AYRISMA</th>
<th class="hide-m">FUNDING</th>
<th class="l">ETIKET</th>
</tr>
</thead>
<tbody id="tbody">
<tr><td colspan="6" class="empty">Yukleniyor...</td></tr>
</tbody>
</table>

<div class="info">
<b>NE ISE YARAR?</b> Binance futures'taki tum USDT coinleri tarar; <b>account (kalabalik)</b> ile <b>position (para/top trader)</b> arasinda ayrisma yasayanlari listeler.<br><br>
&bull; <b>AYRISMA = position - account.</b> Pozitif = para kalabaliktan daha long (whale long, retail short).<br>
&bull; <b>FUNDING CELISKISI</b> (turkuaz): para yonu ile funding ters. Orn. whale long ama funding negatif - en dikkat cekici sinyal.<br>
&bull; Sadece |ayrisma| >= 5 olanlar gosterilir. 30dk'da bir otomatik taranir.<br>
&bull; Veri Binance public API (weight=0 endpoint'ler). Bu arac finansal tavsiye degildir.
</div>
</div>
<script>
let currentPeriod = '1h';
let pollTimer = null;

function z(n){ return String(n).padStart(2,'0'); }

function tick() {
  const d = new Date();
  document.getElementById('clockUTC').textContent =
    `${z(d.getUTCDate())}.${z(d.getUTCMonth()+1)}.${d.getUTCFullYear()} ${z(d.getUTCHours())}:${z(d.getUTCMinutes())}:${z(d.getUTCSeconds())} UTC`;
  const tr = new Date(d.getTime() + 3*3600*1000);
  document.getElementById('clockTR').textContent =
    `${z(tr.getUTCDate())}.${z(tr.getUTCMonth()+1)}.${tr.getUTCFullYear()} ${z(tr.getUTCHours())}:${z(tr.getUTCMinutes())}:${z(tr.getUTCSeconds())} TR`;
}
setInterval(tick, 1000); tick();

function applyTheme(light) {
  document.body.classList.toggle('light', light);
  document.getElementById('themeBtn').innerHTML = light ? '☀' : '☽';
  document.querySelector('meta[name=theme-color]').setAttribute('content', light ? '#f4f6f5' : '#0a0e0d');
  try { localStorage.setItem('scr_theme', light ? 'light' : 'dark'); } catch {}
}
document.getElementById('themeBtn').addEventListener('click', () => {
  applyTheme(!document.body.classList.contains('light'));
});
(function(){ let l=false; try{ l=localStorage.getItem('scr_theme')==='light'; }catch{}; applyTheme(l); })();

function fmtAge(ts) {
  if (!ts) return 'hic';
  const sec = Math.floor(Date.now()/1000 - ts);
  if (sec < 60) return sec + 'sn once';
  if (sec < 3600) return Math.floor(sec/60) + 'dk once';
  return Math.floor(sec/3600) + 'sa once';
}

function fmtFunding(f) {
  if (f == null) return {t:'—', c:''};
  const s = f >= 0 ? '+' : '';
  return {t: s + f.toFixed(4) + '%', c: f > 0 ? 'fund-pos' : (f < 0 ? 'fund-neg' : '')};
}

function render(data) {
  const tbody = document.getElementById('tbody');
  const status = document.getElementById('status');

  if (data.scanning) {
    status.innerHTML = `<span class="scanning"><span class="spin">⦿</span> Taraniyor... ${data.scanned||0}/${data.total||'?'} coin</span>`;
  } else if (data.error) {
    status.innerHTML = `<span class="err">Hata: ${data.error}</span> &middot; son tarama: ${fmtAge(data.ts)}`;
  } else {
    status.innerHTML = `${data.results.length} ayrisan coin &middot; son tarama: ${fmtAge(data.ts)} &middot; periyot: ${currentPeriod}`;
  }

  const rows = data.results || [];
  if (rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="6" class="empty">${data.scanning ? 'Tarama suruyor, sonuclar birikiyor...' : 'Ayrisma yasayan coin bulunamadi (>=5)'}</td></tr>`;
    return;
  }

  let html = '';
  let lastLevel = null;
  for (const r of rows) {
    if (r.level !== lastLevel) {
      const lblmap = {GUCLU:'GUCLU AYRISMA (15+)', ORTA:'ORTA AYRISMA (10-15)', HAFIF:'HAFIF AYRISMA (5-10)'};
      html += `<tr class="section-row"><td colspan="6">${lblmap[r.level]||r.level}</td></tr>`;
      lastLevel = r.level;
    }
    const f = fmtFunding(r.funding);
    const divCls = r.divergence > 0 ? 'div-pos' : 'div-neg';
    const divSign = r.divergence > 0 ? '+' : '';
    const tagCls = r.divergence > 0 ? 'whale-long' : 'whale-short';
    const conflict = r.fundingConflict ? '<span class="conflict-badge">FUNDING CELISKISI</span>' : '';
    html += `<tr>
      <td class="l coin">${r.symbol}</td>
      <td class="hide-m acc">${r.account.toFixed(1)}%</td>
      <td class="hide-m pos">${r.position.toFixed(1)}%</td>
      <td class="${divCls}">${divSign}${r.divergence.toFixed(1)}</td>
      <td class="hide-m ${f.c}">${f.t}</td>
      <td class="l"><span class="tag ${tagCls}">${r.direction}</span>${conflict}</td>
    </tr>`;
  }
  tbody.innerHTML = html;
}

async function load(forceRun) {
  try {
    const url = forceRun ? `/api/run-scan?period=${currentPeriod}` : `/api/scan?period=${currentPeriod}`;
    const r = await fetch(url);
    const data = await r.json();
    render(data);
    // Tarama suruyorsa veya cache bossa, kisa araliklarla yokla
    if (data.scanning || (!data.ts && !data.error)) {
      clearTimeout(pollTimer);
      pollTimer = setTimeout(() => load(false), 3000);
    }
  } catch (e) {
    document.getElementById('status').innerHTML = `<span class="err">Baglanti hatasi: ${e.message}</span>`;
  }
}

document.querySelectorAll('#tabs button').forEach(b => {
  b.addEventListener('click', () => {
    document.querySelectorAll('#tabs button').forEach(x => x.classList.remove('active'));
    b.classList.add('active');
    currentPeriod = b.dataset.period;
    document.getElementById('tbody').innerHTML = '<tr><td colspan="6" class="empty">Yukleniyor...</td></tr>';
    load(false);
  });
});

document.getElementById('refreshBtn').addEventListener('click', () => load(true));

// Ilk yukleme: cache bossa otomatik tarama tetikle
load(false);
// Dashboard acikken her 60sn cache'i tazele (cron arada doldurmus olabilir)
setInterval(() => { if (!document.hidden) load(false); }, 60000);
</script>
</body>
</html>
'''


MANIFEST_JSON = json.dumps({
    "name": "L/S Divergence Screener", "short_name": "L/S Screener", "start_url": "/",
    "display": "standalone", "background_color": "#0a0e0d", "theme_color": "#0a0e0d",
    "icons": [{
        "src": "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 192 192'%3E%3Crect fill='%230a0e0d' width='192' height='192'/%3E%3Ctext x='96' y='112' font-family='monospace' font-size='44' font-weight='bold' fill='%2300d09c' text-anchor='middle'%3EL/S%3C/text%3E%3C/svg%3E",
        "sizes": "192x192", "type": "image/svg+xml"
    }]
})


def _scan_payload(period):
    st = _scan_state.get(period)
    if not st:
        return {"ok": False, "error": "gecersiz periyot"}
    with _scan_lock:
        return {
            "ok": True, "period": period, "ts": st["ts"],
            "scanning": st["scanning"], "scanned": st["scanned"],
            "total": st["total"], "error": st["error"],
            "results": st["results"],
        }


class ScrHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        sys.stderr.write(f" - {self.address_string()} - {fmt % args}\n")

    def _send(self, status, ctype, body):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status, payload):
        self._send(status, "application/json; charset=utf-8", json.dumps(payload).encode("utf-8"))

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        q = urllib.parse.parse_qs(parsed.query)
        period = (q.get("period", ["1h"])[0] or "1h").strip()
        if period not in ("15m", "1h"):
            period = "1h"

        if path in ("/", "/index.html"):
            self._send(200, "text/html; charset=utf-8", SCREENER_HTML.encode("utf-8")); return
        if path == "/manifest.json":
            self._send(200, "application/json; charset=utf-8", MANIFEST_JSON.encode("utf-8")); return
        if path == "/healthz":
            self._json(200, {"ok": True}); return

        if path == "/api/scan":
            # Cache'ten servis. Bossa ve taranmiyor ise arka planda tarama baslat.
            st = _scan_state[period]
            if st["ts"] == 0 and not st["scanning"]:
                threading.Thread(target=run_scan, args=(period,), daemon=True).start()
                time.sleep(0.3)
            self._json(200, _scan_payload(period)); return

        if path == "/api/run-scan":
            # Cron veya elle YENILE. Taranmiyor ise yeni tarama baslat (arka planda).
            st = _scan_state[period]
            if not st["scanning"]:
                threading.Thread(target=run_scan, args=(period,), daemon=True).start()
                time.sleep(0.3)
            self._json(200, _scan_payload(period)); return

        self._json(404, {"ok": False, "error": "not found"})


class ThreadedServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    print(f"L/S Divergence Screener v1 listening on {HOST}:{PORT}", flush=True)
    try:
        with ThreadedServer((HOST, PORT), ScrHandler) as srv:
            srv.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.", flush=True)


if __name__ == "__main__":
    main()

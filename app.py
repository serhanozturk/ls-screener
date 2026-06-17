"""
L/S DIVERGENCE SCREENER (v3)
=============================
Binance futures'taki TUM USDT coinleri tarar; account(kalabalik) vs
position(para) ayrismasi yasayanlari kademeli olarak listeler.

- Kapsam: tum USDT futures (~400), exchangeInfo'dan otomatik (6 saat cache)
- Periyot: 15m + 1h (ikisi de taranir, ayri cache)
- Cron: 30dk'da bir /api/run-scan tetikler
- weight=0 endpoint'ler (account + position L/S), 1000/5dk limit
- funding: premiumIndex TEK toplu cagri (symbol'suz, weight 10) - 400 ayri cagri YOK
- Filtre: |ayrisma| >= 5 (UYUMLU gizli)
- Ban tracker + retry (v4.1 altyapisi)

v3 BAN DUZELTMELERI:
- premiumIndex tek toplu cagri (weight 10) - eski ban'in sebebi olan 400 cagri kaldirildi
- BATCH 15->5, gruplar arasi throttle 1.5s, batch ICI ban kontrolu (http_get short-circuit)
- /api/scan OTOMATIK tarama tetiklemez (sadece /api/run-scan cron/elle)
- /api/series ban kontrolune bagli
- exchangeInfo 6 saat cache (+ bayat fallback)

v3 YENI: DERINLESEN AYRISMA (sadece 1h, bellekte gecmis) - ardisik taramada
ayni yon + |ayrisma| >= 3 buyume. Ayri filtre + rozet.

Calistirma: python3 scr_app.py
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

# ===== exchangeInfo cache (6 saat) =====
_symbols_cache = {"ts": 0.0, "syms": []}
_EXCHANGE_TTL = 6 * 3600

# ===== Derinlesen ayrisma gecmisi (SADECE 1h, bellekte) =====
# symbol -> son ayrisma degeri. Servis restart'inda silinir (ilk 1-2 tarama bos).
_div_history = {}


def get_usdt_symbols():
    """Tum aktif USDT perpetual futures sembolleri (exchangeInfo, 6 saat cache)."""
    now = time.time()
    if _symbols_cache["syms"] and (now - _symbols_cache["ts"]) < _EXCHANGE_TTL:
        return _symbols_cache["syms"]
    try:
        data = http_get("https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=15)
    except Exception:
        # bayat fallback: eski liste varsa onu kullan (ban/ag hatasi taramayi durdurmasin)
        if _symbols_cache["syms"]:
            return _symbols_cache["syms"]
        raise
    syms = []
    for s in data.get("symbols", []):
        if (s.get("quoteAsset") == "USDT"
                and s.get("contractType") == "PERPETUAL"
                and s.get("status") == "TRADING"):
            syms.append(s["symbol"])
    if syms:
        _symbols_cache["syms"] = syms
        _symbols_cache["ts"] = now
    return syms


def get_all_funding():
    """TUM sembollerin funding'i TEK toplu cagri (premiumIndex symbol'suz, weight 10).
    Donus: {SYMBOL: funding_yuzde}. Hata/ban'da bos dict (funding None gosterilir)."""
    out = {}
    try:
        data = http_get("https://fapi.binance.com/fapi/v1/premiumIndex", timeout=15)
    except Exception:
        return out
    if isinstance(data, list):
        for d in data:
            sym = d.get("symbol")
            fr = d.get("lastFundingRate")
            if sym and fr not in (None, ""):
                try:
                    out[sym] = float(fr) * 100
                except Exception:
                    pass
    return out


def _fetch_pair(sym, period, funding_map):
    """Bir coin icin account + position (ikisi de weight=0). Funding toplu map'ten.
    Donus dict veya None."""
    acc = pos = None
    # Account ratio (kalabalik) - weight 0
    try:
        j = http_get(f"https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol={sym}&period={period}&limit=1")
        if isinstance(j, list) and j:
            acc = float(j[-1]["longAccount"]) * 100
    except Exception:
        pass
    # Position ratio (para - top trader) - weight 0
    try:
        j = http_get(f"https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol={sym}&period={period}&limit=1")
        if isinstance(j, list) and j:
            pos = float(j[-1]["longAccount"]) * 100
    except Exception:
        pass
    if acc is None or pos is None:
        return None
    # Funding: per-coin cagri YOK; toplu premiumIndex map'inden (zaten yuzde)
    funding = funding_map.get(sym)

    diff = pos - acc  # pozitif = para daha long = whale long / retail short
    return {
        "symbol": sym.replace("USDT", ""),
        "account": round(acc, 2),
        "position": round(pos, 2),
        "divergence": round(diff, 2),
        "funding": round(funding, 4) if funding is not None else None,
    }


def fetch_series(sym, period, limit=48):
    """Bir coin icin account + position zaman serisi (grafik icin).
    Donus: {ok, account:[{t,v}], position:[{t,v}]} veya {ok:False}."""
    if _binance_banned():
        return {"ok": False, "error": "Binance gecici banli, grafik alinamadi"}
    symbol = sym.upper()
    if not symbol.endswith("USDT"):
        symbol += "USDT"
    acc_series = []
    pos_series = []
    try:
        j = http_get(f"https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol={symbol}&period={period}&limit={limit}")
        if isinstance(j, list):
            for d in j:
                acc_series.append({"t": int(d["timestamp"]), "v": round(float(d["longAccount"]) * 100, 2)})
    except Exception as e:
        return {"ok": False, "error": str(e)}
    try:
        j = http_get(f"https://fapi.binance.com/futures/data/topLongShortPositionRatio?symbol={symbol}&period={period}&limit={limit}")
        if isinstance(j, list):
            for d in j:
                pos_series.append({"t": int(d["timestamp"]), "v": round(float(d["longAccount"]) * 100, 2)})
    except Exception as e:
        return {"ok": False, "error": str(e)}
    if not acc_series or not pos_series:
        return {"ok": False, "error": "veri yok"}
    return {"ok": True, "symbol": sym.replace("USDT", ""), "period": period,
            "account": acc_series, "position": pos_series}


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


def _is_deepening(prev_div, cur_div, threshold=3.0):
    """Derinlesen ayrisma: ardisik taramada AYNI YON + magnitude >= threshold buyume.
    -30 -> -35 = VAR (5 derinlesti). -30 -> -25 = YOK (daraldi). +11 -> -5 = YOK (ters donus).
    prev_div None ise (ilk kez goruluyor) = YOK."""
    if prev_div is None:
        return False
    # ters donus: isaret degisti -> derinlesme degil
    if (prev_div > 0) != (cur_div > 0):
        return False
    return (abs(cur_div) - abs(prev_div)) >= threshold


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

        # FUNDING: tek toplu cagri (premiumIndex symbol'suz, weight 10) - 400 ayri cagri YOK
        funding_map = get_all_funding()
        if _binance_banned():
            st["error"] = "Binance banli (funding cagrisi), tarama iptal"
            return {"ok": False, "error": st["error"]}

        results = []
        # Gruplar halinde + throttle. BATCH 5 (v3). account/position weight=0 ama nazik ol.
        BATCH = 5
        for i in range(0, len(syms), BATCH):
            # Batch ICI ban kontrolu: ban gelirse aninda dur (http_get de short-circuit eder)
            if _binance_banned():
                st["error"] = "Binance banli, tarama yarida kesildi"
                break
            batch = syms[i:i+BATCH]
            with ThreadPoolExecutor(max_workers=BATCH) as ex:
                futs = {ex.submit(_fetch_pair, s, period, funding_map): s for s in batch}
                for f in as_completed(futs):
                    r = f.result()
                    if r and abs(r["divergence"]) >= 5:
                        results.append(r)
            st["scanned"] = min(i + BATCH, len(syms))
            time.sleep(1.5)  # gruplar arasi throttle (v3: 0.4 -> 1.5)
        # Ayrisma buyuklugune gore sirala (mutlak deger, buyukten kucuge)
        results.sort(key=lambda r: abs(r["divergence"]), reverse=True)

        # Derinlesen ayrisma sadece 1h (bellekteki onceki tarama ile kiyas)
        if period == "1h":
            with _scan_lock:
                prev_hist = dict(_div_history)
        else:
            prev_hist = {}

        for r in results:
            lvl, direction = _label(r["divergence"])
            r["level"] = lvl
            r["direction"] = direction
            r["fundingConflict"] = _funding_conflict(r["divergence"], r["funding"])
            if period == "1h":
                prev = prev_hist.get(r["symbol"])
                r["deepening"] = _is_deepening(prev, r["divergence"])
                r["deepenDelta"] = round(abs(r["divergence"]) - abs(prev), 1) if r["deepening"] else None
            else:
                r["deepening"] = False
                r["deepenDelta"] = None

        with _scan_lock:
            st["results"] = results
            st["ts"] = time.time()
            # gecmisi guncelle (1h): bu taramadaki ayrisma degerlerini sakla
            if period == "1h":
                for r in results:
                    _div_history[r["symbol"]] = r["divergence"]
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
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.0/chart.umd.min.js"></script>
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
.controls { display:flex; gap:10px; align-items:center; margin-bottom:14px; flex-wrap:wrap; }
.tabs { display:flex; gap:4px; }
.tabs button { background:transparent; border:1px solid var(--border); color:var(--text-dim);
font-family:inherit; font-size:12px; padding:8px 18px; cursor:pointer; letter-spacing:0.08em; border-radius:0; }
.tabs button.active { background:var(--green); color:var(--bg); border-color:var(--green); font-weight:700; }
.search { flex:1; min-width:130px; background:var(--bg-2); border:1px solid var(--border); color:var(--text);
font-family:inherit; font-size:12px; padding:8px 12px; border-radius:0; }
.search:focus { outline:none; border-color:var(--green); }
.filter-btn { background:transparent; border:1px solid var(--border-strong); color:var(--text-dim);
font-family:inherit; font-size:11px; padding:8px 14px; cursor:pointer; letter-spacing:0.05em; border-radius:0; white-space:nowrap; }
.filter-btn.active { background:var(--accent); color:var(--bg); border-color:var(--accent); font-weight:700; }
.refresh-btn { background:transparent; border:1px solid var(--border-strong); color:var(--text);
font-family:inherit; font-size:11px; padding:8px 14px; cursor:pointer; letter-spacing:0.05em; border-radius:0; white-space:nowrap; }
.refresh-btn:hover { border-color:var(--green); color:var(--green); }
.status { font-size:11px; color:var(--text-dim); margin-bottom:14px; min-height:16px; }
.status .scanning { color:var(--amber); }
.status .err { color:var(--red); }
table { width:100%; border-collapse:collapse; font-size:12px; }
thead th { text-align:right; padding:8px 10px; color:var(--text-dim); font-size:10px;
letter-spacing:0.08em; text-transform:uppercase; border-bottom:1px solid var(--border); font-weight:500;
cursor:pointer; user-select:none; white-space:nowrap; }
thead th:hover { color:var(--text); }
thead th.l { text-align:left; }
thead th .arr { color:var(--green); font-size:9px; margin-left:2px; }
tbody td { padding:9px 10px; text-align:right; border-bottom:1px solid var(--border); }
tbody td.l { text-align:left; }
tbody tr.data-row { cursor:pointer; }
tbody tr.data-row:hover { background:var(--bg-2); }
tbody tr.data-row.open { background:var(--bg-2); }
.coin { font-weight:700; font-size:13px; letter-spacing:0.03em; }
.coin .caret { color:var(--text-faint); font-size:9px; margin-right:5px; display:inline-block; transition:transform 0.2s; }
tr.open .coin .caret { transform:rotate(90deg); color:var(--green); }
.acc { color:var(--text-dim); }
.pos { color:var(--amber); }
.div-pos { color:var(--green); font-weight:700; }
.div-neg { color:var(--red); font-weight:700; }
.fund-pos { color:var(--green); }
.fund-neg { color:var(--red); }
.lvl { font-size:10px; letter-spacing:0.05em; }
.lvl.GUCLU { color:var(--red); font-weight:700; }
.lvl.ORTA { color:var(--amber); }
.lvl.HAFIF { color:var(--text-dim); }
.tag { display:inline-block; font-size:9px; padding:2px 7px; border:1px solid; letter-spacing:0.04em; white-space:nowrap; }
.tag.whale-long { color:var(--green); border-color:var(--green); }
.tag.whale-short { color:var(--red); border-color:var(--red); }
.conflict-badge { display:inline-block; font-size:9px; padding:2px 6px; margin-left:6px;
background:var(--accent); color:var(--bg); font-weight:700; letter-spacing:0.03em; }
.deepen-badge { display:inline-block; font-size:9px; padding:2px 6px; margin-left:5px;
background:var(--amber); color:var(--bg); font-weight:700; letter-spacing:0.03em; }
.filter-btn:disabled { opacity:0.32; cursor:not-allowed; }
.filter-btn:disabled:hover { background:transparent; border-color:var(--border-strong); color:var(--text-dim); }
.chart-row td { padding:0; border-bottom:1px solid var(--border-strong); background:var(--bg); }
.chart-box { padding:16px 10px; }
.chart-box.loading { text-align:center; color:var(--text-faint); padding:30px; font-size:11px; }
.chart-canvas-wrap { position:relative; height:240px; }
.chart-legend { display:flex; gap:18px; justify-content:center; margin-top:10px; font-size:10px; color:var(--text-dim); }
.chart-legend span { display:flex; align-items:center; gap:5px; }
.lg-line { width:16px; height:2px; display:inline-block; }
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
tbody td { padding:8px 5px; font-size:11px; }
thead th { padding:7px 5px; font-size:9px; }
.tabs button { padding:8px 13px; }
.chart-canvas-wrap { height:200px; }
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
<input type="text" class="search" id="search" placeholder="Coin ara (orn. BTC)...">
<button class="filter-btn" id="conflictBtn" title="Sadece funding celiskisi">&#9889; CELISKI</button>
<button class="filter-btn" id="deepenBtn" title="Derinlesen ayrisma (sadece 1H)">&#8675; DERINLESEN</button>
<button class="refresh-btn" id="refreshBtn">&#8635; YENILE</button>
</div>

<div class="status" id="status">Yukleniyor...</div>

<table>
<thead>
<tr id="headrow">
<th class="l" data-sort="symbol">COIN<span class="arr"></span></th>
<th class="hide-m" data-sort="account">HESAP<span class="arr"></span></th>
<th class="hide-m" data-sort="position">POZISYON<span class="arr"></span></th>
<th data-sort="absdiv">AYRISMA<span class="arr">&#9660;</span></th>
<th class="hide-m" data-sort="funding">FUNDING<span class="arr"></span></th>
<th data-sort="level">SEVIYE<span class="arr"></span></th>
<th class="l hide-m">ETIKET</th>
</tr>
</thead>
<tbody id="tbody">
<tr><td colspan="7" class="empty">Yukleniyor...</td></tr>
</tbody>
</table>

<div class="info">
<b>NE ISE YARAR?</b> Tum USDT futures coinlerinde <b>account (kalabalik)</b> vs <b>position (para/top trader)</b> ayrismasini tarar.<br><br>
&bull; <b>AYRISMA = position - account.</b> Pozitif = para kalabaliktan daha long (whale long).<br>
&bull; <b>&#9889; CELISKI</b> butonu: para yonu funding'le ters olanlari suzer (en dikkat cekici sinyal).<br>
&bull; <b>&#8675; DERINLESEN</b> (sadece 1H): ardisik taramada ayni yon + en az 3 buyuyen ayrismalari suzer. Sarı rozet ayrisma derinlestigini gosterir.<br>
&bull; Kolon basliklarina tiklayarak sirala. Coine tiklayarak grafigini ac.<br>
&bull; |ayrisma| >= 5 olanlar; 30dk'da bir taranir. Finansal tavsiye degildir.
</div>
</div>
<script>
let currentPeriod = '1h';
let pollTimer = null;
let allResults = [];
let sortKey = 'absdiv';
let sortDir = 'desc';
let searchTerm = '';
let conflictOnly = false;
let deepenOnly = false;
let openCoin = null;
let chartInstance = null;

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
  if (chartInstance && openCoin) { drawChart(openCoin._lastSeries); }
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

const LEVEL_ORDER = { GUCLU:3, ORTA:2, HAFIF:1 };

function sortRows(rows) {
  const dir = sortDir === 'asc' ? 1 : -1;
  return rows.slice().sort((a, b) => {
    let va, vb;
    switch (sortKey) {
      case 'symbol': va = a.symbol; vb = b.symbol; return va.localeCompare(vb) * dir;
      case 'account': va = a.account; vb = b.account; break;
      case 'position': va = a.position; vb = b.position; break;
      case 'absdiv': va = Math.abs(a.divergence); vb = Math.abs(b.divergence); break;
      case 'funding': va = a.funding==null?-999:a.funding; vb = b.funding==null?-999:b.funding; break;
      case 'level': va = LEVEL_ORDER[a.level]||0; vb = LEVEL_ORDER[b.level]||0;
                    if (va===vb){ return (Math.abs(b.divergence)-Math.abs(a.divergence)); } break;
      default: va = Math.abs(a.divergence); vb = Math.abs(b.divergence);
    }
    return (va - vb) * dir;
  });
}

function visibleRows() {
  let rows = allResults;
  if (conflictOnly) rows = rows.filter(r => r.fundingConflict);
  if (deepenOnly) rows = rows.filter(r => r.deepening);
  if (searchTerm) rows = rows.filter(r => r.symbol.toLowerCase().includes(searchTerm));
  return sortRows(rows);
}

function renderTable() {
  const tbody = document.getElementById('tbody');
  const rows = visibleRows();
  if (rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="7" class="empty">${allResults.length ? 'Filtreye uyan coin yok' : 'Ayrisan coin bulunamadi'}</td></tr>`;
    return;
  }
  let html = '';
  for (const r of rows) {
    const f = fmtFunding(r.funding);
    const divCls = r.divergence > 0 ? 'div-pos' : 'div-neg';
    const divSign = r.divergence > 0 ? '+' : '';
    const tagCls = r.divergence > 0 ? 'whale-long' : 'whale-short';
    const conflict = r.fundingConflict ? '<span class="conflict-badge" title="Funding celiskisi">⚡</span>' : '';
    const deepen = r.deepening ? `<span class="deepen-badge" title="Derinlesen ayrisma">⇣${r.deepenDelta!=null?r.deepenDelta:''}</span>` : '';
    const isOpen = openCoin && openCoin.symbol === r.symbol;
    html += `<tr class="data-row${isOpen?' open':''}" data-coin="${r.symbol}">
      <td class="l coin"><span class="caret">▶</span>${r.symbol}${conflict}${deepen}</td>
      <td class="hide-m acc">${r.account.toFixed(1)}%</td>
      <td class="hide-m pos">${r.position.toFixed(1)}%</td>
      <td class="${divCls}">${divSign}${r.divergence.toFixed(1)}</td>
      <td class="hide-m ${f.c}">${f.t}</td>
      <td class="lvl ${r.level}">${r.level}</td>
      <td class="l hide-m"><span class="tag ${tagCls}">${r.direction}</span></td>
    </tr>`;
    if (isOpen) {
      html += `<tr class="chart-row" data-chart="${r.symbol}"><td colspan="7">
        <div class="chart-box loading" id="chartBox"><span class="spin">⦿</span> Grafik yukleniyor...</div>
      </td></tr>`;
    }
  }
  tbody.innerHTML = html;

  document.querySelectorAll('tr.data-row').forEach(tr => {
    tr.addEventListener('click', () => toggleCoin(tr.dataset.coin));
  });
  if (openCoin && openCoin._lastSeries) drawChart(openCoin._lastSeries);
}

function updateHeaderArrows() {
  document.querySelectorAll('#headrow th').forEach(th => {
    const arr = th.querySelector('.arr');
    if (!arr) return;
    if (th.dataset.sort === sortKey) arr.innerHTML = sortDir === 'asc' ? '▲' : '▼';
    else arr.innerHTML = '';
  });
}

async function toggleCoin(sym) {
  if (openCoin && openCoin.symbol === sym) { openCoin = null; renderTable(); return; }
  openCoin = { symbol: sym, _lastSeries: null };
  renderTable();
  try {
    const r = await fetch(`/api/series?symbol=${sym}&period=${currentPeriod}`);
    const data = await r.json();
    if (!openCoin || openCoin.symbol !== sym) return;
    if (data.ok) { openCoin._lastSeries = data; drawChart(data); }
    else {
      const box = document.getElementById('chartBox');
      if (box) { box.classList.remove('loading'); box.innerHTML = `<div style="color:var(--red);padding:20px;text-align:center">Grafik verisi alinamadi: ${data.error||''}</div>`; }
    }
  } catch (e) {
    const box = document.getElementById('chartBox');
    if (box) box.innerHTML = `<div style="color:var(--red);padding:20px;text-align:center">Hata: ${e.message}</div>`;
  }
}

function cssVar(n){ return getComputedStyle(document.body).getPropertyValue(n).trim(); }

function drawChart(data) {
  const box = document.getElementById('chartBox');
  if (!box) return;
  box.classList.remove('loading');
  const green = cssVar('--green'), amber = cssVar('--amber'), dim = cssVar('--text-dim'), grid = cssVar('--border');
  box.innerHTML = `<div class="chart-canvas-wrap"><canvas id="lsChart"></canvas></div>
    <div class="chart-legend">
      <span><span class="lg-line" style="background:${dim}"></span>HESAP (kalabalik)</span>
      <span><span class="lg-line" style="background:${amber}"></span>POZISYON (para)</span>
    </div>`;
  const labels = data.account.map(p => {
    const d = new Date(p.t + 3*3600*1000);
    return `${z(d.getUTCDate())}.${z(d.getUTCMonth()+1)} ${z(d.getUTCHours())}:${z(d.getUTCMinutes())}`;
  });
  if (chartInstance) { chartInstance.destroy(); chartInstance = null; }
  const ctx = document.getElementById('lsChart').getContext('2d');
  chartInstance = new Chart(ctx, {
    type: 'line',
    data: { labels, datasets: [
      { label:'Hesap', data:data.account.map(p=>p.v), borderColor:dim, backgroundColor:'transparent', borderWidth:1.5, pointRadius:0, tension:0.25 },
      { label:'Pozisyon', data:data.position.map(p=>p.v), borderColor:amber, backgroundColor:'transparent', borderWidth:2, pointRadius:0, tension:0.25 }
    ]},
    options: {
      responsive:true, maintainAspectRatio:false, interaction:{mode:'index',intersect:false},
      plugins:{ legend:{display:false}, tooltip:{ backgroundColor:cssVar('--bg-2'), titleColor:cssVar('--text'),
        bodyColor:cssVar('--text'), borderColor:cssVar('--border-strong'), borderWidth:1, padding:8,
        callbacks:{ label:(c)=>`${c.dataset.label}: ${c.parsed.y}%` } } },
      scales:{ x:{ grid:{color:grid}, ticks:{color:dim,maxRotation:0,autoSkip:true,maxTicksLimit:6,font:{size:9}} },
        y:{ grid:{color:grid}, ticks:{color:dim,font:{size:9},callback:(v)=>v+'%'} } }
    }
  });
}

function render(data) {
  const status = document.getElementById('status');
  allResults = data.results || [];
  if (data.scanning) {
    status.innerHTML = `<span class="scanning"><span class="spin">⦿</span> Taraniyor... ${data.scanned||0}/${data.total||'?'} coin</span>`;
  } else if (data.error) {
    status.innerHTML = `<span class="err">Hata: ${data.error}</span> &middot; son tarama: ${fmtAge(data.ts)}`;
  } else if (!data.ts) {
    status.innerHTML = `Henuz tarama yok &middot; <b style="color:var(--green)">YENILE</b>'ye bas veya 30dk cron'u bekle`;
  } else {
    const cf = allResults.filter(r=>r.fundingConflict).length;
    const dp = allResults.filter(r=>r.deepening).length;
    let extra = `${cf} funding celiskisi`;
    if (currentPeriod === '1h') extra += ` &middot; ${dp} derinlesen`;
    status.innerHTML = `${allResults.length} ayrisan coin &middot; ${extra} &middot; son tarama: ${fmtAge(data.ts)}`;
  }
  renderTable();
}

async function load(forceRun) {
  try {
    const url = forceRun ? `/api/run-scan?period=${currentPeriod}` : `/api/scan?period=${currentPeriod}`;
    const r = await fetch(url);
    const data = await r.json();
    render(data);
    // v3: sadece tarama suruyorken poll. Bos cache'te (ts=0, scanning=false) sonsuz poll YOK.
    if (data.scanning) {
      clearTimeout(pollTimer);
      pollTimer = setTimeout(() => load(false), 3000);
    }
  } catch (e) {
    document.getElementById('status').innerHTML = `<span class="err">Baglanti hatasi: ${e.message}</span>`;
  }
}

// Kolon siralama
document.querySelectorAll('#headrow th[data-sort]').forEach(th => {
  th.addEventListener('click', () => {
    const k = th.dataset.sort;
    if (sortKey === k) sortDir = sortDir === 'asc' ? 'desc' : 'asc';
    else { sortKey = k; sortDir = (k === 'symbol') ? 'asc' : 'desc'; }
    updateHeaderArrows(); renderTable();
  });
});

// Periyot sekmeleri
document.querySelectorAll('#tabs button').forEach(b => {
  b.addEventListener('click', () => {
    document.querySelectorAll('#tabs button').forEach(x => x.classList.remove('active'));
    b.classList.add('active');
    currentPeriod = b.dataset.period;
    openCoin = null;
    updateDeepenBtn();
    document.getElementById('tbody').innerHTML = '<tr><td colspan="7" class="empty">Yukleniyor...</td></tr>';
    load(false);
  });
});

// Arama
document.getElementById('search').addEventListener('input', (e) => {
  searchTerm = e.target.value.trim().toLowerCase();
  renderTable();
});

// Celiski filtre
document.getElementById('conflictBtn').addEventListener('click', () => {
  conflictOnly = !conflictOnly;
  document.getElementById('conflictBtn').classList.toggle('active', conflictOnly);
  renderTable();
});

// Derinlesme filtre (sadece 1H)
function updateDeepenBtn() {
  const btn = document.getElementById('deepenBtn');
  if (currentPeriod !== '1h') {
    deepenOnly = false;
    btn.classList.remove('active');
    btn.disabled = true;
  } else {
    btn.disabled = false;
  }
}
document.getElementById('deepenBtn').addEventListener('click', () => {
  if (currentPeriod !== '1h') return;
  deepenOnly = !deepenOnly;
  document.getElementById('deepenBtn').classList.toggle('active', deepenOnly);
  renderTable();
});

document.getElementById('refreshBtn').addEventListener('click', () => load(true));

updateHeaderArrows();
updateDeepenBtn();
load(false);
setInterval(() => { if (!document.hidden && !openCoin) load(false); }, 60000);
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
            # v3: SADECE cache servis. Otomatik tarama YOK (sayfa acmak ban tetiklemesin).
            # Tarama yalnizca /api/run-scan (cron) veya elle YENILE ile baslar.
            self._json(200, _scan_payload(period)); return

        if path == "/api/run-scan":
            # Cron veya elle YENILE. Taranmiyor ise yeni tarama baslat (arka planda).
            st = _scan_state[period]
            if not st["scanning"]:
                threading.Thread(target=run_scan, args=(period,), daemon=True).start()
                time.sleep(0.3)
            self._json(200, _scan_payload(period)); return

        if path == "/api/series":
            sym = (q.get("symbol", [""])[0] or "").strip()
            if not sym:
                self._json(400, {"ok": False, "error": "symbol gerekli"}); return
            res = fetch_series(sym, period)
            self._json(200, res); return

        self._json(404, {"ok": False, "error": "not found"})


class ThreadedServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main():
    print(f"L/S Divergence Screener v3 listening on {HOST}:{PORT}", flush=True)
    try:
        with ThreadedServer((HOST, PORT), ScrHandler) as srv:
            srv.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.", flush=True)


if __name__ == "__main__":
    main()

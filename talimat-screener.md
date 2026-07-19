# SERHAN — LS DIVERGENCE SCREENER (Proje Talimatlari)

## ==========================================================
## #1 KIRMIZI CIZGI — BINANCE BAN (HER SEYIN ONUNDE)
## ==========================================================
BINANCE BAN BIZIM KIRMIZI CIZGIMIZDIR. Kod yazarken, guncellerken veya
ozellik eklerken HER ZAMAN once "bu degisiklik ban yedirir mi?" diye sor.
Ban riski olan hicbir kod, ozellik ne kadar degerli olursa olsun, YAZILMAZ.

Kod yazmadan ONCE ban kontrol listesi (her seferinde zihinden gecir):
1. Bu degisiklik Binance'e KAC yeni istek ekler? (coin basina mi, toplu mu?)
2. Endpoint weight'i ve OZEL limiti ne?
3. futures/data ailesi (account+position+openInterestHist) ortak 1000/5dk limitine
   tabi — coin basina istek sayisi x ~400 coin < 900 olmali (pay birak).
4. Yeni bir per-coin cagri ekliyorsam: TOPLU alternatifi var mi?
5. Tarama suresi uzasa bile ban GARANTILI engelleniyor mu?

Ban riski belirsizse: ONCE kullaniciya soyle, esik/limit hesabini birlikte yap,
ondan sonra yaz. Asla "muhtemelen sorun olmaz" deme.

## KIMLIK VE ILETISIM
- Turkce yanit ver. Net, oz, dogrudan ol — gereksiz uzatma.
- Sistem etiketleri / kod ciktilari ASCII-safe olmali (Turkce ozel karakter YOK).
- Swing trader (cogunlukla 1g-1hafta, 1h'de acilis). Perpetual futures'ta deneyimli.

## EN ONEMLI KURAL — IZIN OLMADAN KOD YAZMA
- "yaz", "basla", "devam" gibi ACIK onay gelmeden KOD YAZMA/degistirme.
- Once tasarimi tartis, netlestir, soru sor. Onay gelince yaz.
- Tek seferde bir soru sor; ask_user_input ile secenekli sor (mobil kolayligi).

## SADECE DEGISECEK YERE DOKUN
- Bir duzeltme/ekleme yaparken SADECE o degisiklikle ilgili satirlari degistir.
- "Iyilestirmek/temizlemek" icin istenmeden degisiklik YAPMA.
- Degisiklik oncesi: bu satir baska neyi etkiler? Yan etki var mi? KONTROL ET.
- Degisiklikten sonra: degismemesi gereken seyler AYNI MI test et.

## BU CHAT: SADECE SCREENER
Bu chat yalnizca Screener icin kullanilir.
Engine ve Terminal AYRI chatlerde, AYRI talimatlarda konusulur — ASLA karistirma.

## GITHUB = GERCEK KAYNAK (kod isine baslamadan once CEK)
- Repo PUBLIC: https://github.com/serhanozturk/ls-screener
- Guncel kod:    https://raw.githubusercontent.com/serhanozturk/ls-screener/main/Screener.py
- Guncel talimat: https://raw.githubusercontent.com/serhanozturk/ls-screener/main/talimat-screener.md
- Kod degisikligine baslamadan ONCE Screener.py'yi bu adresten cek (curl ile) —
  project knowledge'daki kopya ESKI olabilir, GitHub'daki deploy edilen gercek koddur.
- Talimatta suphe varsa (surum uyusmazligi vb.) talimati da GitHub'dan cek.
- Claude GitHub'a YAZAMAZ — commit'i kullanici yapar (web UI veya ileride Claude Code).

## PROJE BILGILERI
- **Dosya:** Screener.py (isim ASLA degismez)
- **Repo:** ls-screener
- **Altyapi:** Hetzner VPS Nuremberg, Coolify, Port Mapping 8766:8766
- **Erisim:** http://178.104.143.245:8766 (sslip.io telefonda engelli — IP:port kullan)
- **Start:** python Screener.py
- **Guncel surum:** v13

### Surum gecmisi (ozet)
- v3: Ban duzeltmeleri (premiumIndex toplu cagri, batch 5, throttle, exchangeInfo 6h cache)
- v4: Erken sinyal (patlama/dusus, OI degisimi)
- v5: Kayan pencere rate limiter (_fd_throttle, 900/5dk)
- v6: OI esigi %2→%10
- v7: PATLAMA icin OI>=%10 ZORUNLU KAPI
- v8: Telegram bildirimi (1h patlama)
- v9: /api/run-scan kucuk response ({"ok":true}) — cron "output too large" duzeltmesi
- v10: TELEGRAM_CHAT_IDS coklu liste (virgullu; Serhan + Osman/6445815704)
- v11: PATLAMA kapisi VEYA mantigi (son mum VEYA 3 mum kumulatif; 1h %10, 15m %5),
  15m bildirimi + 2h dedup, baslikta tag (1H/15M), mesajda fiyat
- v12: ERKEN UYARI sistemi (hacim + taker buy patlamasi, ayri hafif cron)
- v13: Supabase sinyal kaydi (backtest icin, screener_signals tablosu)

## NE YAPAR
Tum USDT futures coinleri (~400) tarar; account(kalabalik) vs position(para)
ayrismasi + ERKEN SINYAL (patlama/dusus) + ERKEN UYARI (hacim/tb patlamasi).
- Kapsam: exchangeInfo'dan otomatik (quoteAsset=USDT, PERPETUAL, TRADING; 6h cache)
- Periyot: 15m + 1h (ayri sekme, ayri cache)
- Filtre: |ayrisma| >= 5 VEYA patlama (OI kapisi) VEYA dusus skoru >= 2

## ENDPOINT'LER
- / → dashboard UI
- /healthz → keep-alive
- /api/scan?period=15m|1h → cache servis (bossa arka planda tarar); 15m'de "early" alani da doner
- /api/run-scan?period= → cron/elle tetikler, kucuk JSON doner (BUYUK JSON DEGIL)
- /api/early-check → ERKEN UYARI kontrolu (hafif, 10-30sn), kucuk JSON doner
- /api/series?symbol=X&period= → grafik zaman serisi (ban kontrolune bagli)

## ERKEN SINYAL MANTIGI (PATLAMA / DUSUS)
- **PATLAMA kapisi (v11 VEYA mantigi):** son kapanmis mum OI >= esik VEYA
  son 3 kapanmis mum KUMULATIF OI >= esik. Esik: 1h=%10, 15m=%5.
  (openInterestHist limit=4, ayni istek — kademeli birikim de ani sicrama da yakalanir)
  Kapi gecilirse: OI tek basina skor 1. Whale long (+1), funding<=0 (+1). Toplam 1-3.
- **DUSUS:** whale short(+1) + OI<=-%2(+1) + funding>=%0.05(+1). Skor>=2 aday.
- Esikler: OI_PUMP_MIN=10.0, OI_PUMP_MIN_15M=5.0, OI_DUMP_MIN=2.0, FUNDING_HIGH=0.05
- Sistem KAPANMIS mumlari olcer — canli mumdaki hareket bir sonraki mum kapaninca gorulur.

## ERKEN UYARI SISTEMI (v12) — hareket baslangici tespiti
Kural (hepsi VE, 15m son KAPANMIS mum):
1. Mum hacmi (USDT) >= 5x son 8 mum ortalamasi (EARLY_VOL_X)
2. Taker buy (USDT) >= 5x son 8 mum ortalamasi (EARLY_TB_X)
3. Mum hacmi >= 100K USDT (EARLY_MIN_VOL, olu coin filtresi)
FIYAT SARTI YOK (gizli toplama kacmasin — hacim fiyattan once patlar).
"Onceki tek mum" DEGIL "8 mum ortalamasi" tabani: kademeli buyumede carpan
yalan soylemesin diye (B coin dersi: tek mum carpani 2-3x kalir, ortalamaya gore 5x+).

2 asamali veri akisi (ban):
- 1. asama: bulk ticker/24hr (TEK cagri, weight 40) → taramalar arasi quoteVolume
  deltasi → delta >= 3x kendi ortalamasi VE >= 40K → aday (genis ag, max 30 aday)
- 2. asama: sadece adaylara klines 15m limit=10 (weight 1/coin) → kesin kontrol
- Toplam ~40-70 weight/kontrol; futures/data butcesine DOKUNMAZ. Ilk kontrol warmup.

Dogrulama (4 coin): AKE atesleme mumu OK, BANK atesleme OK, B kalkistan 2 saat once OK,
TLM dipten donus mumu OK (+%77 oncesi).

Bildirim: "ERKEN UYARI — 15M" + fiyat + carpanlar. 2h dedup. Sessiz dusur
(teyit gelmezse ek mesaj yok). PATLAMA gelirse mesajina TEYIT satiri eklenir
(uyari saati + o anki fiyat + o zamandan beri % degisim; _early_alerts 6h tutulur).
Dashboard 15m sekmesinde ERKEN UYARI kutusu.

## SUPABASE SINYAL KAYDI (v13, backtest)
- Her EARLY ve PATLAMA sinyali aninda screener_signals tablosuna TEK INSERT.
- Dedup'a takilanlar DA kaydedilir — notified kolonu ayirt eder.
- Env: SUPABASE_URL + SUPABASE_KEY (service_role; Engine ile ayni proje/degerler).
  Env yoksa SESSIZCE atlanir, davranis degismez.
- RLS disabled (Engine kalibi). Sonuc olcumu kayit aninda YAPILMAZ —
  backtest analizinde toptan cekilir (Secenek A: analiz aninda klines'tan +1h/+4h/+24h).
- Supabase'e gider, Binance'e degil → ban etkisi SIFIR.
- 1 hafta veri birikince: "backtest yap" → sinyaller Supabase'den, fiyatlar klines'tan,
  sinyal tipi bazinda isabet/getiri raporu.

## KAYAN PENCERE RATE LIMITER (_fd_throttle)
- futures/data ailesi (account+position+openInterestHist) ortak 1000 istek/5dk IP limiti
- _FD_WINDOW=300s, _FD_MAX=900. Her futures/data istegi oncesi son 5dk sayar.
- Tavanda otomatik bekler. Tarama ~7-8dk surer — normal.
- klines ve ticker futures/data AILESINDE DEGIL — genel weight limitine tabi (2400/dk).

## TELEGRAM BILDIRIMI
- Env: TELEGRAM_TOKEN, TELEGRAM_CHAT_IDS (virgullu coklu liste; KODA GOMULU DEGIL).
- Her chat_id'ye AYRI istek (biri basarisiz olsa digeri etkilenmez).
- Bildirim tipleri: PATLAMA — 1H (dedup yok), PATLAMA — 15M (2h dedup, skor artarsa istisna),
  ERKEN UYARI — 15M (2h dedup). Hepsinde son fiyat var. DUSUS bildirimi YOK.
- Telegram'a gider, Binance'e degil → ban riski YOK.
- chat_id: getUpdates JSON'unda "chat":{"id":...} degeri ("update_id" DEGIL!).
- Bot, /start yazilmamis kullaniciya mesaj ATAMAZ. Yeni kisi once bota /start yazar,
  getUpdates'ten chat_id alinir, TELEGRAM_CHAT_IDS'e virgulle eklenir.
- parse_mode HTML: <b> dengeli olmali. Negatif deger formatlari {v:+.1f} ile
  (elle "+" on eki koyma — "+-3.8%" hatasi cikar, v11'de yasandi).

## CRON CIZELGESI (cron-job.org)
- Tarama 1h: /api/run-scan?period=1h → dakika 1,31
- Tarama 15m: /api/run-scan?period=15m → dakika 13,43
- ERKEN UYARI: /api/early-check → dakika 3,18,33,48 (mum kapanisindan ~3dk sonra)
- Keep-alive: /healthz → dakika 11,21,41,51
- "Pause on failure" KAPALI birakilmali (cron kendini durdurmasin).
- Cron endpoint'leri KUCUK JSON donmeli — buyuk cevap "output too large" ile
  cron'u devre disi birakir (v9 dersi).
- cron-job.org: Schedule → Custom → Minutes alanina virgullu deger, Hours/Days = *

NOT: Hetzner VPS her zaman ayakta (Render gibi uyumaz) → keep-alive sadece
cron-job.org'un "failure on silence" durumunu onlemek icin.

## BINANCE API KURALLARI
- **Ban tracker zorunlu:** 418/429 yiyince yerel takip, Binance'e gitmeyi kes.
  Retry-After header VARSA tam suresine uy (cap yok); yoksa default 30dk.
  Header'i kisa cap'e KIRPMA — uzun banlarda erken istek atip bani UZATIR.
- **Endpoint weight'leri:**
  - globalLongShortAccountRatio = 0 | topLongShortPositionRatio = 0 | openInterestHist = 0
  - Bu uc "futures/data" endpoint'i ORTAK 1000 istek/5dk IP limitine tabi!
  - premiumIndex symbol ILE = 1 → 400 coin icin AYRI cagri YAPMA!
  - premiumIndex symbol'SUZ = 10 → TUM funding + markPrice tek cagrida (tercih et)
  - ticker/24hr symbol'SUZ = 40 → tum coinlerin fiyat/hacim ozeti tek cagrida
  - klines limit<100 = 1 → SADECE on elemeden gecen adaylara (max 30)
  - exchangeInfo = 1 (6 saat cache'le)
- **Genel weight limiti:** 2400 weight/dakika/IP.
- **Eskalasyonlu ban:** 2dk → 3 gune kadar (tekrar suclu uzar).
- **Ban kalkma:** Coolify'dan servisi durdur → sure dolar → yeniden baslat.
  Banliyken istek atmak sureyi UZATIR.
- **Screener ban gecmisi:**
  (1) premiumIndex 400 ayri cagri → 10h ban → v3 toplu cagri cozdu.
  (2) v4 OI eklendi → 800→1200 istek → 1000/5dk asti → v5 kayan pencere cozdu.

## TEKNIK STANDARTLAR
- Python STANDART KUTUPHANE ONLY. pip install YOK. requirements.txt yok.
- HTML/CSS/JS Screener.py icine gomulu (tek dosya).
- Tum borsa cagrilari: ban tracker + cache + retry zorunlu.
- Thread guvenligi: global state Lock ile korunmali.
- HTML gomerken JS emoji'leri: \U0001F680 gibi 8-hane veya dogrudan emoji kullan
  (\u{...} JS formati Python'da gecersiz).
- Gomulu JS degisince node --check ile syntax dogrula (py_compile JS hatasi gormez).
- **GECE/GUNDUZ MODU STANDART:** CSS degiskenleri (:root koyu + body.light acik),
  header'da tema butonu (ay/gunes ikonu), localStorage (scr_theme), varsayilan KOYU.
  Grafik varsa tema-duyarli olmali (CSS degiskeninden renk oku, tema degisince yeniden ciz).

## IS AKISI ("yaz" gelince)
1. GUNCEL kodu GitHub'dan cek: curl raw.githubusercontent.com/serhanozturk/ls-screener/main/Screener.py
   → /home/claude/Screener.py (project knowledge kopyasini KULLANMA, eski olabilir)
2. Sadece degisecek satirlari duzenle (str_replace)
3. python3 -m py_compile ile syntax kontrol (+ JS degistiyse node --check)
4. Birim testler (mantik fonksiyonlari mock ile)
5. Gercek server testi (http.client localhost) — Binance 403/418 container'da NORMAL
6. /mnt/user-data/outputs/Screener.py'ye kopyala (dd if= of= tercih)
7. present_files ile sun

## DEPLOY (kullaniciya hatirlat)
- GitHub ls-screener reposu → Screener.py → tumunu sec → sil → yeni kodu yapistir → TEK commit
- Coolify otomatik deploy eder (Dockerfile mevcut)
- Deploy sonrasi loglarda "v13 listening" gor = dogrulama
- Erisim: http://178.104.143.245:8766
- Env listesi (Coolify): TELEGRAM_TOKEN, TELEGRAM_CHAT_IDS, SUPABASE_URL, SUPABASE_KEY

## BEKLEYEN / ACIK KONULAR
- 1 hafta Supabase verisi biriksin → backtest analizi (EARLY ve PATLAMA performansi,
  esik kalibrasyonu: 5x/5x/100K ve OI %10/%5 gozden gecirilecek).
- ERKEN UYARI ilk kontrol warmup — restart sonrasi ilk 15dk sinyal cikmaz, normal.
- Derinlesen ayrisma UI'da var (1h); ayri gelistirme gerekmiyor.

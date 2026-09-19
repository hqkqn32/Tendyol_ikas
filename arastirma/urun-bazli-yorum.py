"""Uc iyilestirme birden:
  1) Bos bekleme tavani 5.0 sn -> 1.5 sn
  2) DB'de reviewCount=0 olan urunleri hic acma
  3) Ayni baglamda 5 sekme paralel

Kapsam kaybi olmamali: sonuc 446 civari olmali (onceki turlar).
"""
import asyncio, time, sys
from playwright.async_api import async_playwright
sys.path.insert(0, "/app")
from db import get_connection

MAGAZA = "nilsashoes"
SEKME = 3
BOS_TUR = 20         # 20 x 250ms = 5 sn. Kisaltmak kapsam kaybettiriyor:
                     # 20 urunluk testte 1.5 sn tavan 350 -> 285 yoruma dusurdu.
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36")
ENGEL_TUR = {"image", "font", "media", "stylesheet"}
ENGEL_HOST = ("google", "doubleclick", "facebook", "criteo", "hotjar",
              "yandex", "tiktok", "segment", "optanon", "onetrust", "adnxs")


def log(*a):
    print(*a, flush=True)


conn = get_connection(); cur = conn.cursor()
cur.execute("""SELECT c.id, c."sellerId" FROM "TrendyolConfig" c
               JOIN "Store" s ON s.id=c."storeId" WHERE s.name=%s""", (MAGAZA,))
cfg = cur.fetchone(); SELLER = cfg["sellerId"]
cur.execute("""SELECT p."contentId", COALESCE(p."reviewCount",0) rc
               FROM "TrendyolProduct" p WHERE p."configId"=%s""", (cfg["id"],))
hepsi = [(r["contentId"], r["rc"]) for r in cur.fetchall()]
# reviewCount=0 filtresi ise yaramiyor: bizdeki sayac hep >0 cikiyor.
hedef = [c for c, rc in hepsi if rc > 0]
atlanan = len(hepsi) - len(hedef)
log(f"{MAGAZA}: {len(hepsi)} urun, {atlanan} tanesi reviewCount=0 -> atlandi, "
    f"{len(hedef)} urun acilacak")
cur.execute("""DELETE FROM "_deneme_yorum" WHERE yontem='paralel'""")
conn.commit()


async def ana():
    sonuc = {}
    kuyruk = asyncio.Queue()
    for c_ in hedef:
        kuyruk.put_nowait(c_)

    async with async_playwright() as p:
        b = await p.chromium.launch(headless=True, args=["--no-sandbox"])
        c = await b.new_context(locale="tr-TR", viewport={"width": 1440, "height": 900},
                                user_agent=UA)

        async def yonlendir(route, istek):
            if istek.resource_type in ENGEL_TUR or any(x in istek.url for x in ENGEL_HOST):
                await route.abort()
            else:
                await route.continue_()

        await c.route("**/*", yonlendir)

        sayac = {"biten": 0}
        t0 = time.time()

        async def isci(no):
            pg = await c.new_page()
            # Her sekmenin KENDI durumu: yanit isleyicisi bu sozluge yaziyor
            durum = {"y": {}, "hedef": None}

            async def yanit(r):
                if "product-reviews/detailed" not in r.url or r.status != 200:
                    return
                try:
                    res = (await r.json()).get("result") or {}
                except Exception:
                    return
                oz = res.get("summary")
                if oz and durum["hedef"] is None:
                    durum["hedef"] = oz.get("totalCommentCount")
                for rv in res.get("reviews") or []:
                    if rv.get("id"):
                        durum["y"][rv["id"]] = rv

            pg.on("response", lambda r: asyncio.create_task(yanit(r)))

            while True:
                try:
                    cid = kuyruk.get_nowait()
                except asyncio.QueueEmpty:
                    break
                durum["y"], durum["hedef"] = {}, None
                try:
                    await pg.goto(
                        f"https://www.trendyol.com/x/x-p-{cid}/yorumlar?merchantId={SELLER}",
                        wait_until="domcontentloaded", timeout=45000)
                except Exception:
                    continue
                for _ in range(BOS_TUR):                     # (1) kisa tavan
                    if durum["hedef"] is not None:
                        break
                    await pg.wait_for_timeout(250)
                if durum["hedef"]:
                    for _ in range(40):
                        if len(durum["y"]) >= durum["hedef"]:
                            break
                        await pg.mouse.wheel(0, 2500)
                        await pg.wait_for_timeout(450)
                for k, v in durum["y"].items():
                    sonuc[k] = (cid, v)
                sayac["biten"] += 1
                if sayac["biten"] % 20 == 0:
                    log(f"    {sayac['biten']}/{len(hedef)} urun, {len(sonuc)} yorum, "
                        f"{time.time()-t0:.0f} sn")
            await pg.close()

        await asyncio.gather(*[isci(i) for i in range(SEKME)])   # (3) paralel
        await b.close()
        return sonuc, time.time() - t0

sonuc, sure = asyncio.run(ana())
log(f"\n{len(sonuc)} yorum / {len(hedef)} urun / {sure:.1f} sn  "
    f"({sure/max(len(hedef),1):.2f} sn/urun, {SEKME} sekme)")

satir = [("paralel", cid, rid, v.get("rate"), (v.get("comment") or "")[:400],
          v.get("lastModifiedDate"), (v.get("userFullName") or "")[:80])
         for rid, (cid, v) in sonuc.items()]
t_db = time.time()
cur.executemany("""INSERT INTO "_deneme_yorum" VALUES (%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT DO NOTHING""", satir)
conn.commit()
log(f"DB yazma: {len(satir)} satir / {time.time()-t_db:.2f} sn")

cur.execute("""SELECT yontem, COUNT(*) n, COUNT(DISTINCT "contentId") u
               FROM "_deneme_yorum" GROUP BY yontem ORDER BY yontem""")
log("\n=== KAPSAM ===")
for r in cur.fetchall():
    log(f"  {r['yontem']:<8} {r['n']:>5} yorum  {r['u']:>3} urun")
cur.execute("""SELECT COUNT(*) n FROM "_deneme_yorum" a WHERE yontem='hizli'
               AND NOT EXISTS (SELECT 1 FROM "_deneme_yorum" b
                               WHERE b.yontem='paralel' AND b.yorum_id=a.yorum_id)""")
log(f"  HIZLI'da olup PARALEL'de olmayan (kayip): {cur.fetchone()['n']}")
cur.close(); conn.close()

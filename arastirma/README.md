# Arastirma — kullanilmiyor

Bu klasordeki kod **uretimde calismiyor**. Olculmus bir yontemi ve
sayilarini kaybetmemek icin duruyor.

## urun-bazli-yorum.py — tum yorumlari cekme

Bugun canlida kullandigimiz yontem satici akisi (`review_scraper.py`):
tek uc nokta, hizli, ama **kayan bir pencere** donduruyor. nilsashoes'ta
olculen: 180 yorum / 89 gun (2026-06-22 -> 2026-09-19). Yogun magazalarda
gecmis kayboluyor; yeni kurulan magaza ilk gun sadece son ~90 gunu aliyor.

Bu script urun urun gezerek o sinirin otesine geciyor.

### Nasil calisiyor

Uc nokta dogrudan cagrilamiyor:

- `requests` ile taklit -> **403**. Trendyol'un WAF'i TLS parmak izine
  bakiyor; Python/OpenSSL el sikismasi Chrome'unki gibi degil.
  (Satici akisi bu denetimi yapmiyor, bu uc nokta yapiyor.)
- Sayfa icinden `fetch` -> **418**. Sayfanin kendi JS'i geciyor, bizimki
  gecmiyor; uretilen ek bir imza var.

Calisan tek yol: sayfayi acip **kendi istegini attirmak** ve yaniti
`response` olayindan yakalamak.

URL bicimi kritik:

    https://www.trendyol.com/x/x-p-{contentId}/yorumlar?merchantId={sellerId}

Slug uydurma olabilir (`/x/x-`), onemli olan `-p-{contentId}` ve
`/yorumlar`. Gercek urun sayfasi (`/marka/urun-adi-p-ID`) yorum modulunu
yuklemiyor; `/yorumlar` yuklu­yor.

Yanit yapisi: `result.reviews` (dizi), `result.summary.totalCommentCount`
(hedef sayi), `result.summary.totalPages`. Asagi kaydirdikca sayfa
`page=0,1,2...` diye kendisi ilerliyor.

### Olculen (nilsashoes, 91 urun, sunucuda)

| Yontem | Yorum | Sure |
|---|---|---|
| Satici akisi (canli) | 170 | 6.9 sn |
| Urun bazli, 1 sekme | 446 | 512.5 sn |
| + kaynak engelleme, erken durma | 446 | 231.0 sn |
| + 3 sekme paralel | 432 | 175.6 sn |

DB'de o gun 411 yorum vardi — yani bu yontem **tek calistirmada**, aylarca
biriktirdigimizden fazlasini getiriyor.

Surenin dagilimi (1 sekme, 91 urun / 221 sn):

- ilk yaniti bekleme %64  (yorumsuz urunde 5.16 sn, yorumluda 0.96 sn)
- sayfa yukleme        %29
- kaydirma             %6   — 73 urunde sifir; 20 yorumun altinda tek
                              sayfada geliyor

### Bilinen eksikler

- **Hiz/kapsam takasi.** Sabit bekleme tavanini kisaltmak veya sekme
  sayisini artirmak yanit kacirtiyor: 20 urunluk testte 1 sekme/5sn = 350
  yorum, 1 sekme/1.5sn = 285, 5 sekme/5sn = 314, 3 sekme/5sn = 332.
  Dogrusu sabit tavan degil olay tabanli bekleme (`wait_for_response`).
- **Tarih alani eslenmemis.** Urun uc noktasi tarihi `lastModifiedDate`
  disinda bir alanda donduruyor; script onu okumuyor, `tarih` bos kaliyor.
- **Olcek.** ~5000 urunluk katalogda saatler surer. Gecelik taramaya
  uygun degil; olsa olsa kurulumda bir kez.
- **`reviewCount=0` filtresi ise yaramiyor** — bizdeki sayac hep >0.

### Not: urun listesi uc noktasi

`/integration/product/sellers/{id}/products` (v1) **brownout'ta**:
HTTP 426, "Servisinizi Product v2'ye tasiyin". Araliklı calisiyor.
Barkod eslestirmesi bu uc noktaya bagli, v2'ye gecis gerekiyor.

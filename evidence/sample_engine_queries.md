# Sample Engine Queries, Business Insights, Cross-Layer Reconciliation & Rerun Idempotency Evidence

Dokumen ini menyajikan hasil eksekusi query analitik dari layer **Gold PostgreSQL** (`gold.order_360`, `gold.customer_daily`, `gold.product_daily`, `gold.channel_campaign_daily`, dan `gold.executive_kpis_daily`), definisi terstandarisasi metrik **`refund_rate`** dan **`return_rate`**, pemisahan tegas antara **Temuan Data (Fakta Observasi)** dan **Hipotesis Bisnis**, serta bukti **Rekonsiliasi Lintas Layer (`Bronze` $\rightarrow$ `Silver` $\rightarrow$ `Gold`)** dan **Kesamaan Hasil Rerun (*Idempotency Proof*)** yang dijalankan dari kondisi **database kosong (`0` tabel)** untuk memproses **10.000 resolved orders** selama periode **22 Juni 2026 – 19 September 2026 (90 hari bisnis)**.

---

## Standarisasi Definisi Metrik Pengembalian (`refund_rate` vs. `return_rate`)

Sesuai kontrak metrik pada `README.md` dan `engine/llm/catalog.py`, pipeline memisahkan secara ketat metrik pengembalian dana (*refund*) dan pengembalian barang (*return*):

| Nama Metrik | Formula Eksplisit (SQL / Kontrak Gold) | Pembilang (*Numerator*) | Penyebut (*Denominator*) | Nilai Agregat 90 Hari |
|---|---|---|---|---:|
| **`refund_rate`** *(Kontrak Gold)* | `COUNT(order_id) FILTER (WHERE refunded_amount > 0)::NUMERIC / NULLIF(COUNT(order_id) FILTER (WHERE captured_payment_amount > 0), 0)` | Pesanan dengan event `REFUND_COMPLETED` (`refunded_amount > 0`): **1.991 pesanan** | Pesanan dengan pembayaran berhasil di-*capture* (`captured_payment_amount > 0`): **8.776 pesanan** | **0.2269 (22.69%)** |
| **`order_refund_share`** *(Proporsi terhadap Total Order)* | `COUNT(order_id) FILTER (WHERE refunded_amount > 0)::NUMERIC / NULLIF(COUNT(order_id), 0)` | Pesanan dengan event `REFUND_COMPLETED` (`refunded_amount > 0`): **1.991 pesanan** | Seluruh *resolved orders* di `gold.order_360`: **10.000 pesanan** | **0.1991 (19.91%)** |
| **`return_rate`** *(Kontrak Gold)* | `COUNT(order_id) FILTER (WHERE return_status <> 'none')::NUMERIC / NULLIF(COUNT(order_id), 0)` | Pesanan yang memiliki event retur terverifikasi di `silver.return_events` (`return_status IN ('requested', 'received', 'closed')`): **1.372 pesanan** | Seluruh *resolved orders* di `gold.order_360`: **10.000 pesanan** | **0.1372 (13.72%)** *(Rata-rata harian: **0.1371**)* |

> **Catatan Konsistensi Definisi:**
> 1. Kolom `return_status` pada `gold.order_360` diturunkan **murni dari keberadaan event di `silver.return_events`** (`'closed'`, `'received'`, `'requested'`, atau `'none'`), tidak lagi dicampur dengan status header `orders.status = 'returned'` yang tidak memiliki jejak event retur. Dengan demikian, jumlah pesanan berstatus retur di `gold.order_360` (**1.372 pesanan**) dan `SUM(return_count)` di `gold.customer_daily` (**1.372**) merekonsiliasi **100% tepat (`selisih = 0`)** terhadap `COUNT(DISTINCT order_id)` pada `silver.return_events` (**1.372 pesanan**).
> 2. Kolom `refunded_units` pada `gold.product_daily` (**7.478 unit**) dihitung hanya dari item pada pesanan yang benar-benar memiliki pengembalian dana selesai (`order_refunded_amount > 0`), diproporsikan terhadap nilai transaksi sebelum refund.

---

## 1. Performa Pendapatan, AOV, Refund Rate, dan Return Rate per Sales Channel (`gold.order_360`)

**Pertanyaan Analitik:** *What is the total net revenue, average order value, contract refund rate (over captured-payment orders), order refund share (over all orders), and return rate by sales channel?*

**SQL Query:**
```sql
SELECT
    sales_channel,
    COUNT(order_id) AS total_orders,
    COUNT(order_id) FILTER (WHERE captured_payment_amount > 0) AS captured_orders,
    COUNT(order_id) FILTER (WHERE refunded_amount > 0) AS refunded_orders,
    COUNT(order_id) FILTER (WHERE return_status <> 'none') AS returned_orders,
    SUM(gross_merchandise_value) AS gmv,
    SUM(discount_amount) AS discounts,
    SUM(shipping_revenue) AS shipping,
    SUM(captured_payment_amount) AS captured_payment,
    SUM(refunded_amount) AS refunded_amount,
    SUM(net_revenue) AS net_revenue,
    ROUND(AVG(net_revenue), 2) AS avg_order_value,
    ROUND(
        COUNT(order_id) FILTER (WHERE refunded_amount > 0)::NUMERIC
        / NULLIF(COUNT(order_id) FILTER (WHERE captured_payment_amount > 0), 0),
        4
    ) AS refund_rate_captured,
    ROUND(
        COUNT(order_id) FILTER (WHERE refunded_amount > 0)::NUMERIC
        / NULLIF(COUNT(order_id), 0),
        4
    ) AS order_refund_share,
    ROUND(
        COUNT(order_id) FILTER (WHERE return_status <> 'none')::NUMERIC
        / NULLIF(COUNT(order_id), 0),
        4
    ) AS return_rate
FROM gold.order_360
GROUP BY sales_channel
ORDER BY net_revenue DESC;
```

**Hasil Query (Data Aktual Terproses):**

| sales_channel | total_orders | captured_orders | refunded_orders | returned_orders | gmv | discounts | shipping | captured_payment | refunded_amount | net_revenue | avg_order_value | refund_rate_captured | order_refund_share | return_rate |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **WEB** | 2,507 | 2,192 | 512 | 362 | 3,885,039.33 | 41,547.00 | 11,203.00 | 3,362,861.94 | 413,887.11 | **3,440,808.22** | **1,372.48** | 0.2336 (23.36%) | 0.2042 (20.42%) | 0.1444 (14.44%) |
| **STORE** | 2,517 | 2,214 | 490 | 328 | 3,747,674.80 | 41,794.50 | 11,403.50 | 3,270,055.79 | 363,409.34 | **3,353,874.46** | **1,332.49** | **0.2213 (22.13%)** | **0.1947 (19.47%)** | **0.1303 (13.03%)** |
| **MOBILE_APP** | 2,494 | 2,201 | 500 | 340 | 3,748,013.32 | 41,068.00 | 11,032.00 | 3,279,784.78 | 377,163.93 | **3,340,813.39** | **1,339.54** | 0.2272 (22.72%) | 0.2005 (20.05%) | 0.1363 (13.63%) |
| **MARKETPLACE** | 2,482 | 2,169 | 489 | 342 | 3,719,455.09 | 41,605.50 | 11,259.00 | 3,225,285.63 | 394,779.83 | **3,294,328.76** | **1,327.29** | 0.2254 (22.54%) | 0.1970 (19.70%) | 0.1378 (13.78%) |
| **TOTAL** | **10,000** | **8,776** | **1,991** | **1,372** | **15,100,182.54** | **166,015.00** | **44,897.50** | **13,137,988.14** | **1,549,240.21** | **13,429,824.83** | **1,342.98** | **0.2269 (22.69%)** | **0.1991 (19.91%)** | **0.1372 (13.72%)** |

**Query Diagnostik Tambahan Lintas Kanal (`gold.order_360` + `silver.support_events`):**
```sql
SELECT
    f.sales_channel,
    COUNT(DISTINCT f.order_id) AS total_orders,
    ROUND(AVG(f.promotion_count), 4) AS avg_promos_per_order,
    ROUND(SUM(f.discount_amount) / NULLIF(SUM(f.gross_merchandise_value), 0), 4) AS discount_to_gmv_ratio,
    COUNT(DISTINCT f.order_id) FILTER (WHERE f.order_status = 'cancelled') AS cancelled_orders,
    COUNT(DISTINCT se.ticket_id) AS support_tickets,
    ROUND(COUNT(DISTINCT se.ticket_id)::NUMERIC / NULLIF(COUNT(DISTINCT f.order_id), 0), 4) AS support_tickets_per_order
FROM gold.order_360 f
LEFT JOIN silver.support_events se ON se.order_id = f.order_id
GROUP BY f.sales_channel
ORDER BY total_orders DESC;
```

| sales_channel | total_orders | avg_promos_per_order | discount_to_gmv_ratio | cancelled_orders | support_tickets | support_tickets_per_order |
|---|---:|---:|---:|---:|---:|---:|
| **STORE** | 2,517 | 1.5827 | 0.0112 (1.12%) | 301 | **437** | **0.1736 (17.36%)** |
| **WEB** | 2,507 | 1.5711 | 0.0107 (1.07%) | 313 | 387 | 0.1544 (15.44%) |
| **MOBILE_APP** | 2,494 | 1.5761 | 0.0109 (1.09%) | 290 | 405 | 0.1624 (16.24%) |
| **MARKETPLACE** | 2,482 | 1.5688 | 0.0112 (1.12%) | 311 | 390 | 0.1571 (15.71%) |

### Pemisahan Temuan Data (Fakta) vs. Hipotesis Bisnis

- **Temuan Data (Fakta Observasi):**
  1. Volume pesanan terdistribusi merata di keempat kanal (`2.482`–`2.517` pesanan per kanal), dengan **`WEB`** menghasilkan *Net Revenue* tertinggi (**3.440.808,22**) dan *Average Order Value (AOV)* tertinggi (**1.372,48**).
  2. Kanal **`STORE`** memiliki `refund_rate` (atas pesanan *captured*) paling rendah (**22,13%** atau `490 / 2.214`), `order_refund_share` paling rendah (**19,47%** atau `490 / 2.517`), nilai nominal *refund* paling rendah (**363.409,34**, lebih rendah **50.477,77** dibanding `WEB`), serta `return_rate` paling rendah (**13,03%** atau `328 / 2.517` dibanding `WEB` **14,44%**).
  3. Sebaliknya, pemeriksaan diagnostik menunjukkan bahwa kanal **`STORE`** justru memiliki rasio tiket bantuan (`support_tickets_per_order`) paling tinggi (**17,36%** atau **437 tiket**, dibanding `WEB` **15,44%** atau **387 tiket**).

- **Hipotesis Bisnis (Belum Dapat Disimpulkan Hanya dari Perbandingan Rate):**
  > Alasan mengapa kanal `STORE` memiliki *refund rate* dan *return rate* lebih rendah **tidak dapat disimpulkan secara kausal hanya dari perbandingan persentase rate antar-kanal**. Beberapa hipotesis yang mungkin menjelaskan fenomena ini dan memerlukan pengujian lebih lanjut:
  1. **Hipotesis H1 (Inspeksi Fisik Pra-Pembelian):** Pelanggan di gerai fisik dapat melihat dan memeriksa kondisi/ukuran barang secara langsung sebelum transaksi selesai, sehingga menekan ketidakcocokan produk pasca-pembelian.
  2. **Hipotesis H2 (Friksi Proses Retur Offline vs. Kemudahan Retur Online):** Rendahnya *return/refund rate* di `STORE` yang disertai tingginya jumlah tiket komplain (`437` tiket support) dapat mengindikasikan adanya *return friction* — pelanggan toko fisik yang mengalami kendala menghubungi *support*, tetapi enggan melakukan retur karena harus datang kembali secara fisik ke toko, berbeda dengan kanal `WEB` yang menyediakan alur pengembalian barang digital.
  3. **Hipotesis H3 (Perbedaan Bauran Kategori / *Product Mix*):** Kanal `STORE` mungkin menjual proporsi produk dengan risiko retur rendah (mis. *grocery* atau *beauty*) lebih banyak dibandingkan kanal `WEB`.

- **Langkah Validasi Lanjutan:**
  - Membedah distribusi `reason` pada `silver.support_events` khusus pesanan `STORE` vs `WEB` (apakah keluhan didominasi *damaged item*, *sizing*, atau prosedur pengembalian).
  - Melakukan *stratified analysis* `refund_rate` dan `return_rate` pada pasangan `(sales_channel, active_category)` melalui join `gold.order_360`, `silver.order_items`, dan `silver.product_categories`.

---

## 2. Efektivitas Campaign & Return on Ad Spend / ROAS (`gold.channel_campaign_daily`)

**Pertanyaan Analitik:** *Which marketing campaigns achieved the best ROAS, and how do attributed orders and net revenue compare against campaign spend?*

**SQL Query:**
```sql
SELECT
    campaign_id,
    SUM(campaign_spend) AS total_spend,
    SUM(attributed_orders) AS attributed_orders,
    SUM(attributed_customers) AS attributed_customers,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_revenue) AS net_revenue,
    SUM(refunds) AS refunds,
    ROUND(SUM(net_revenue) / NULLIF(SUM(campaign_spend), 0), 4) AS roas,
    ROUND(AVG(conversion_rate), 4) AS avg_conversion_rate
FROM gold.channel_campaign_daily
GROUP BY campaign_id
HAVING SUM(campaign_spend) > 0
ORDER BY roas DESC;
```

**Hasil Query (Data Aktual Terproses):**

| campaign_id | total_spend | attributed_orders | attributed_customers | gross_revenue | net_revenue | refunds | roas | avg_conversion_rate |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| **CMP-04** | 22,321.34 | 754 | 754 | 1,170,811.13 | 1,038,790.65 | 122,837.48 | **46.5380x** | 0.8215 |
| **CMP-05** | 22,623.13 | 783 | 783 | 1,197,538.72 | 1,038,100.63 | 149,297.09 | **45.8867x** | 0.8099 |
| **CMP-02** | 21,681.63 | 728 | 728 | 1,107,220.80 | 983,784.32 | 114,621.48 | **45.3741x** | 0.8035 |
| **CMP-09** | 23,088.87 | 752 | 752 | 1,157,055.88 | 1,047,305.58 | 100,830.80 | **45.3598x** | 0.8024 |
| **CMP-01** | 22,511.56 | 758 | 758 | 1,157,243.43 | 1,018,626.10 | 129,526.83 | **45.2490x** | 0.7840 |
| **CMP-06** | 22,094.05 | 751 | 751 | 1,128,259.42 | 981,614.93 | 137,682.99 | **44.4289x** | 0.7948 |
| **CMP-08** | 24,028.01 | 773 | 772 | 1,152,513.69 | 1,024,645.89 | 118,323.30 | **42.6438x** | 0.8095 |
| **CMP-07** | 23,721.85 | 726 | 726 | 1,099,943.77 | 986,291.51 | 105,102.76 | **41.5773x** | 0.8118 |
| **CMP-03** | 24,327.65 | 719 | 718 | 1,073,971.04 | 973,296.88 | 91,626.16 | **40.0078x** | 0.7994 |
| **CMP-10** | 25,043.21 | 739 | 738 | 1,107,949.86 | 983,493.88 | 115,981.98 | **39.2719x** | 0.8203 |

*(Catatan Rekonsiliasi: Selain 10 campaign berbayar di atas yang mencakup **7.483 pesanan** dan total spend **231.441,30**, terdapat **2.517 pesanan organik `STORE` (`UNATTRIBUTED`)** dengan `campaign_spend = 0.00` dan `net_revenue = 3.353.874,46`, sehingga total `attributed_orders` di `gold.channel_campaign_daily` merekonsiliasi tepat **10.000 pesanan**).*

### Pemisahan Temuan Data (Fakta) vs. Hipotesis Bisnis

- **Temuan Data (Fakta Observasi):**
  1. **`CMP-04`** mencatat ROAS agregat tertinggi (**46,5380x** dengan *spend* `22.321,34` dan *net revenue* `1.038.790,65`), diikuti **`CMP-05`** (**45,8867x**, volume tertinggi `783` pesanan).
  2. **`CMP-09`** menghasilkan *Net Revenue* tertinggi (**1.047.305,58**) karena nilai *refund* yang relatif rendah (**100.830,80** dibanding `CMP-05` yang mencapai **149.297,09**).
  3. **`CMP-10`** menyerap biaya iklan terbesar (**25.043,21**) tetapi menghasilkan ROAS terendah (**39,2719x**).

- **Hipotesis Bisnis & Langkah Validasi:**
  1. **Hipotesis H1 (Kualitas Targeting vs. Promosi Agresif pada `CMP-05`):** Tingginya pesanan sekaligus tingginya *refund* pada `CMP-05` (`149.297,09`) diduga dipicu oleh pesan promosi yang menarik pembeli impulsif (*high-churn buyers*), sedangkan `CMP-09` menjangkau segmen pelanggan dengan kecocokan produk yang lebih tinggi.
  2. **Hipotesis H2 (Saturasi Audiens pada `CMP-10`):** Biaya iklan `CMP-10` yang paling tinggi dengan ROAS terendah mengindikasikan terjadinya *diminishing marginal returns* pada kanal/audiens targetnya.
  3. **Validasi Lanjutan:** Lakukan uji realokasi anggaran bertahap (*holdout/incrementality test*) sebesar 10–15% dari `CMP-10` ke `CMP-04` dan `CMP-09` sambil memantau marginal ROAS mingguan.

---

## 3. Performa Kategori Produk, Refunded Units, dan Risiko Stockout (`gold.product_daily`)

**Pertanyaan Analitik:** *Which product categories generated the most revenue, refunded units (from completed refunds), and stockout rates using point-in-time SCD2 categories?*

**SQL Query:**
```sql
SELECT
    active_category,
    COUNT(DISTINCT product_id) AS distinct_products,
    SUM(units_sold) AS units_sold,
    SUM(order_count) AS order_lines,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_revenue) AS net_revenue,
    SUM(gross_revenue - net_revenue) AS discount_and_refund_erosion,
    SUM(refunded_units) AS refunded_units,
    ROUND(SUM(refunded_units)::NUMERIC / NULLIF(SUM(units_sold), 0), 4) AS refunded_unit_rate,
    COUNT(*) FILTER (WHERE stockout_flag IS TRUE) AS stockout_product_days,
    ROUND(AVG(stockout_flag::INT) FILTER (WHERE stockout_flag IS NOT NULL), 4) AS stockout_rate
FROM gold.product_daily
GROUP BY active_category
ORDER BY net_revenue DESC;
```

**Hasil Query (Data Aktual Terproses):**

| active_category | distinct_products | units_sold | order_lines | gross_revenue | net_revenue | discount_and_refund_erosion | refunded_units | refunded_unit_rate | stockout_product_days | stockout_rate |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **fashion** | 17 | 13,195 | 5,172 | 3,308,526.87 | **2,916,761.77** | **391,765.10** | 1,629 | **0.1235 (12.35%)** | 85 | 0.0570 (5.70%) |
| **electronics** | 18 | 13,360 | 5,245 | 3,128,788.43 | **2,777,836.29** | 350,952.14 | 1,581 | 0.1183 (11.83%) | 81 | 0.0526 (5.26%) |
| **sports** | 19 | **13,967** | **5,481** | 2,992,623.34 | **2,654,823.41** | 337,799.93 | **1,671** | 0.1196 (11.96%) | **91** | 0.0562 (5.62%) |
| **home** | 13 | 8,518 | 3,420 | 2,264,245.00 | **2,010,010.64** | 254,234.36 | 1,033 | 0.1213 (12.13%) | 69 | **0.0704 (7.04%)** |
| **grocery** | 12 | 8,360 | 3,328 | 2,133,670.33 | **1,883,063.09** | 250,607.24 | 1,015 | 0.1214 (12.14%) | 54 | 0.0551 (5.51%) |
| **beauty** | 7 | 4,979 | 1,989 | 1,272,328.57 | **1,142,447.10** | 129,881.47 | 549 | 0.1103 (11.03%) | 36 | 0.0610 (6.10%) |
| **TOTAL** | **80** *(6 SCD2 transitions)* | **62,379** | **24,635** | **15,100,182.54** | **13,384,942.30** | **1,715,240.24** | **7,478** | **0.1199 (11.99%)** | **416** | **0.0578 (5.78%)** |

*(Catatan Rekonsiliasi: Selisih antara `SUM(net_revenue)` di `gold.order_360` sebesar `13.429.824,83` dan `SUM(net_revenue)` level item di `gold.product_daily` sebesar `13.384.942,30` adalah `44.882,53`, yang merepresentasikan `shipping_revenue` level order sebesar `44.897,50` dengan selisih pembulatan proporsional multi-item sebesar `14,97`).*

### Pemisahan Temuan Data (Fakta) vs. Hipotesis Bisnis

- **Temuan Data (Fakta Observasi):**
  1. Tiga kategori terbesar — **`fashion`** (`2.916.761,77`), **`electronics`** (`2.777.836,29`), dan **`sports`** (`2.654.823,41`) — menyumbang **62,38%** dari total *net revenue* produk.
  2. **`fashion`** memiliki proporsi unit yang di-*refund* tertinggi (**12,35%** atau `1.629` dari `13.195` unit terjual) serta erosi diskon + refund tertinggi (**391.765,10**), sedangkan **`beauty`** memiliki rasio unit *refund* terendah (**11,03%**).
  3. **`home`** mencatat *stockout rate* tertinggi (**7,04%** atau `69 / 980` *product-days*), sementara **`sports`** mengalami frekuensi *stockout* absolut tertinggi (**91 *product-days***).

- **Hipotesis Bisnis & Langkah Validasi:**
  1. **Hipotesis H1 (Ketidakcocokan Ukuran pada `fashion`):** Rasio *refunded units* tertinggi pada `fashion` (`12,35%`) diduga disebabkan oleh ketidaksesuaian ukuran/fit saat pembelian daring, namun perlu diverifikasi apakah SKU tertentu menyumbang mayoritas *refund*.
  2. **Hipotesis H2 (Kehilangan Penjualan Akibat Stockout pada `home`):** Tingginya *stockout rate* `home` (`7,04%`) diduga menahan potensi pertumbuhan GMV kategori tersebut; perlu diuji di lokasi `store_id` mana `available_quantity = 0` paling sering terjadi pada `silver.inventory_snapshots`.

---

## 4. Retensi Pelanggan, Repeat Customer Activity, & Beban Customer Support (`gold.customer_daily`)

**Pertanyaan Analitik:** *How do first-time vs. repeat customer cohorts contribute to total orders, net revenue, returns, and support contacts?*

**SQL Query:**
```sql
SELECT
    CASE
        WHEN repeat_customer_flag = TRUE AND new_customer_flag = FALSE THEN 'Repeat Customer Activity'
        WHEN new_customer_flag = TRUE AND repeat_customer_flag = FALSE THEN 'First-Time Customer Activity'
        ELSE 'Same-Day First & Repeat Activity'
    END AS customer_cohort_activity,
    COUNT(*) AS customer_active_days,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(order_count) AS total_orders,
    SUM(unit_quantity) AS total_units,
    SUM(gross_revenue) AS total_gross_revenue,
    SUM(net_revenue) AS total_net_revenue,
    SUM(refund_amount) AS total_refund_amount,
    SUM(return_count) AS total_returns,
    SUM(support_contacts) AS total_support_contacts,
    ROUND(SUM(net_revenue) / NULLIF(SUM(order_count), 0), 2) AS net_revenue_per_order,
    ROUND(SUM(support_contacts)::NUMERIC / NULLIF(SUM(order_count), 0), 4) AS support_contact_rate
FROM gold.customer_daily
GROUP BY 1
ORDER BY total_net_revenue DESC;
```

**Hasil Query (Data Aktual Terproses):**

| customer_cohort_activity | customer_active_days | unique_customers | total_orders | total_units | total_gross_revenue | total_net_revenue | total_refund_amount | total_returns | total_support_contacts | net_revenue_per_order | support_contact_rate |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **Repeat Customer Activity** | 7,329 | 2,268 | 7,490 | 46,704 | 11,364,942.87 | **10,125,858.37** | 1,148,822.50 | 1,017 | 1,219 | **1,351.92** | 0.1628 (16.28%) |
| **First-Time Customer Activity** | 2,401 | 2,401 | 2,401 | 14,927 | 3,555,956.56 | **3,146,325.00** | 380,218.56 | 339 | 387 | 1,310.42 | 0.1612 (16.12%) |
| **Same-Day First & Repeat Activity** | 54 | 54 | 109 | 748 | 179,283.11 | **157,641.46** | 20,199.15 | 16 | 13 | **1,446.25** | 0.1193 (11.93%) |
| **TOTAL** | **9,784** | **2,455** *(aktif bertransaksi)* | **10,000** | **62,379** | **15,100,182.54** | **13,429,824.83** | **1,549,240.21** | **1,372** | **1,619** | **1,342.98** | **0.1619 (16.19%)** |

### Pemisahan Temuan Data (Fakta) vs. Hipotesis Bisnis

- **Temuan Data (Fakta Observasi):**
  1. Dari `2.455` pelanggan unik yang bertransaksi selama 90 hari (`2.401` pelanggan dengan pesanan pertama pada hari tunggal + `54` pelanggan yang melakukan pesanan pertama sekaligus pesanan kedua di hari yang sama), sebanyak **`2.268` pelanggan (92,38%)** kembali melakukan pembelian ulang (*repeat orders*).
  2. Aktivitas pelanggan berulang (*Repeat Customer Activity*) menyumbang **74,90% dari total pesanan (`7.490` order)** dan **75,40% dari total *net revenue* (`10.125.858,37`)**, dengan *net revenue per order* lebih tinggi (**1.351,92** vs **1.310,42** pada pesanan pertama).
  3. Frekuensi kontak *support* per pesanan antara pelanggan baru (**16,12%**) dan pelanggan berulang (**16,28%**) hampir identik.

- **Hipotesis Bisnis & Langkah Validasi:**
  1. **Hipotesis H1 (Kebutuhan Support Bersifat Transaksional, Bukan Kurva Pembelajaran Pengguna Baru):** Karena `support_contact_rate` pelanggan berulang (`16,28%`) tidak lebih rendah daripada pelanggan baru (`16,12%`), tiket bantuan kemungkinan besar dipicu oleh insiden operasional per pesanan (mis. keterlambatan pengiriman atau proses retur/refund) alih-alih kebingungan pengguna baru saat memakai aplikasi/website.
  2. **Validasi Lanjutan:** Menguji korelasi antara `support_contacts` dan `return_count` / `refund_amount` pada level pesanan di `silver.support_events`.

---

## 5. Tren Executive KPIs Harian & Bulanan (`gold.executive_kpis_daily`)

**Pertanyaan Analitik:** *How do the executive KPIs evolve month-over-month and across the 90-day operating window?*

**SQL Query (Ringkasan 90 Hari & Tren Bulanan):**
```sql
SELECT
    TO_CHAR(DATE_TRUNC('month', metric_date), 'YYYY-MM') AS metric_month,
    COUNT(*) AS days_in_month,
    SUM(total_orders) AS monthly_orders,
    SUM(gross_revenue) AS monthly_gross_revenue,
    SUM(net_revenue) AS monthly_net_revenue,
    ROUND(SUM(net_revenue) / NULLIF(SUM(total_orders), 0), 2) AS weighted_monthly_aov,
    ROUND(AVG(refund_rate), 4) AS avg_daily_refund_rate,
    ROUND(AVG(return_rate), 4) AS avg_daily_return_rate,
    ROUND(AVG(repeat_customer_rate), 4) AS avg_daily_repeat_rate,
    ROUND(AVG(stockout_rate), 4) AS avg_daily_stockout_rate,
    ROUND(AVG(support_contact_rate), 4) AS avg_daily_support_contact_rate
FROM gold.executive_kpis_daily
GROUP BY DATE_TRUNC('month', metric_date)
ORDER BY metric_month ASC;
```

**Hasil Query Tren Bulanan (`gold.executive_kpis_daily`):**

| metric_month | days_in_month | monthly_orders | monthly_gross_revenue | monthly_net_revenue | weighted_monthly_aov | avg_daily_refund_rate | avg_daily_return_rate | avg_daily_repeat_rate | avg_daily_stockout_rate | avg_daily_support_contact_rate |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **2026-06** *(22–30 Jun)* | 9 | 961 | 1,404,100.49 | 1,250,542.40 | 1,301.29 | 0.2394 (23.94%) | 0.1399 (13.99%) | 0.1826 (18.26%) | 0.0639 (6.39%) | 0.1500 (15.00%) |
| **2026-07** *(1–31 Jul)* | 31 | 3,540 | 5,322,392.66 | 4,711,817.33 | 1,331.02 | 0.2335 (23.35%) | 0.1396 (13.96%) | 0.6345 (63.45%) | 0.0573 (5.73%) | 0.1590 (15.90%) |
| **2026-08** *(1–31 Agu)* | 31 | 3,374 | 5,083,907.71 | 4,522,403.30 | 1,340.37 | 0.2230 (22.30%) | 0.1404 (14.04%) | 0.9052 (90.52%) | 0.0504 (5.04%) | 0.1650 (16.50%) |
| **2026-09** *(1–19 Sep)* | 19 | 2,125 | 3,289,781.68 | 2,945,061.80 | **1,385.91** | **0.2166 (21.66%)** | **0.1262 (12.62%)** | **0.9747 (97.47%)** | 0.0678 (6.78%) | 0.1670 (16.70%) |
| **TOTAL / AVG (90 Hari)** | **90** | **10,000** | **15,100,182.54** | **13,429,824.83** | **1,342.98** *(Daily Avg: 1,342.95)* | **0.2269 (22.69%)** | **0.1371 (13.71%)** | **0.7544 (75.44%)** | **0.0578 (5.78%)** | **0.1619 (16.19%)** |

### Pemisahan Temuan Data (Fakta) vs. Hipotesis Bisnis

- **Temuan Data (Fakta Observasi):**
  1. `repeat_customer_rate` meningkat secara monoton dari **18,26%** pada bulan Juni menjadi **63,45%** (Juli), **90,52%** (Agustus), dan **97,47%** (September).
  2. Seiring meningkatnya proporsi pelanggan berulang, `weighted_monthly_aov` naik dari **1.301,29** (Juni) menjadi **1.385,91** (September), sementara `avg_daily_refund_rate` turun dari **23,94%** menjadi **21,66%** dan `avg_daily_return_rate` turun dari **13,99%** menjadi **12,62%**.

- **Hipotesis Bisnis & Langkah Validasi:**
  1. **Hipotesis H1 (Efek Maturitas Kohort Pelanggan Tetap):** Kenaikan `repeat_customer_rate` hingga `97,47%` di bulan September terjadi karena basis pelanggan terdaftar bersifat tetap (`2.500` pelanggan di `silver.customers`), sehingga pada bulan ke-3 hampir seluruh transaksi berasal dari pelanggan yang sudah pernah berbelanja sebelumnya.
  2. **Hipotesis H2 (Pelanggan Berulang Lebih Mengenal Produk):** Penurunan `refund_rate` dan `return_rate` di bulan September diduga berkaitan dengan semakin tingginya proporsi *repeat customers* yang sudah memahami kualitas/ukuran produk, namun perlu diuji apakah penurunan tersebut murni efek kohort atau dipengaruhi perubahan bauran promosi bulanan.

---

## 6. Bukti Rekonsiliasi Lintas Layer (`Bronze` $\rightarrow$ `Silver` $\rightarrow$ `Gold`) & Validasi Tipe Data

### 6.1 Rekonsiliasi Baris `Bronze` $\rightarrow$ `Silver` (Seluruh 19 Tabel Sumber)

Setiap baris mentah yang masuk ke 19 tabel `bronze.raw_*` wajib dipertanggungjawabkan secara eksak ke tabel `silver.*` (*loaded*) atau `silver.rejected_records` (*rejected*) tanpa ada baris yang hilang maupun tergandakan (`row_variance = 0`).

**SQL Query Rekonsiliasi `Bronze` vs `Silver`:**
```sql
SELECT
    b.table_name AS bronze_table,
    s.table_name AS silver_table,
    b.records_loaded AS bronze_rows,
    s.records_loaded AS silver_loaded,
    s.records_rejected AS silver_rejected,
    (b.records_loaded - (s.records_loaded + s.records_rejected)) AS row_variance
FROM ops.layer_load_stats b
JOIN ops.layer_load_stats s
    ON s.pipeline_run_id = b.pipeline_run_id
   AND s.layer = 'silver'
   AND REPLACE(b.table_name, 'bronze.raw_', 'silver.') = s.table_name
WHERE b.pipeline_run_id = 'rerun_verify_002'
  AND b.layer = 'bronze'
ORDER BY b.table_name;
```

**Hasil Aktual Rekonsiliasi 19 Tabel (`ops.layer_load_stats`):**

| No | bronze_table | silver_table | bronze_rows | silver_loaded | silver_rejected | row_variance |
|---:|---|---|---:|---:|---:|---:|
| 1 | `bronze.raw_campaign_spend` | `silver.campaign_spend` | 855 | 855 | 0 | **0** |
| 2 | `bronze.raw_city_reference` | `silver.city_reference` | 8 | 8 | 0 | **0** |
| 3 | `bronze.raw_customer_addresses` | `silver.customer_addresses` | 2,500 | 2,500 | 0 | **0** |
| 4 | `bronze.raw_customer_profiles` | `silver.customer_profiles` | 2,777 | 2,777 | 0 | **0** |
| 5 | `bronze.raw_customers` | `silver.customers` | 2,500 | 2,500 | 0 | **0** |
| 6 | `bronze.raw_inventory_snapshots` | `silver.inventory_snapshots` | 35,041 | 35,040 | 1 | **0** |
| 7 | `bronze.raw_order_items` | `silver.order_items` | 24,960 | 24,956 | 4 | **0** |
| 8 | `bronze.raw_order_promotions` | `silver.order_promotions` | 15,729 | 15,729 | 0 | **0** |
| 9 | `bronze.raw_orders` | `silver.orders` | 10,001 | 10,000 | 1 | **0** |
| 10 | `bronze.raw_payment_events` | `silver.payment_events` | 22,582 | 21,915 | 667 | **0** |
| 11 | `bronze.raw_product_categories` | `silver.product_categories` | 86 | 86 | 0 | **0** |
| 12 | `bronze.raw_products` | `silver.products` | 80 | 80 | 0 | **0** |
| 13 | `bronze.raw_promotions` | `silver.promotions` | 12 | 12 | 0 | **0** |
| 14 | `bronze.raw_refund_events` | `silver.refund_events` | 4,121 | 3,982 | 139 | **0** |
| 15 | `bronze.raw_return_events` | `silver.return_events` | 3,553 | 3,440 | 113 | **0** |
| 16 | `bronze.raw_sales_channels` | `silver.sales_channels` | 4 | 4 | 0 | **0** |
| 17 | `bronze.raw_stores` | `silver.stores` | 5 | 5 | 0 | **0** |
| 18 | `bronze.raw_support_events` | `silver.support_events` | 2,971 | 2,870 | 101 | **0** |
| 19 | `bronze.raw_web_events` | `silver.web_events` | 30,986 | 30,000 | 986 | **0** |
| **TOTAL** | **19 Tabel Bronze** | **19 Tabel Silver** | **158,771** | **156,759** | **2,012** | **0** |

### 6.2 Rincian Baris yang Ditolak ke `silver.rejected_records` (`2.012` Baris)

**SQL Query:**
```sql
SELECT
    source_table,
    rejection_reason,
    COUNT(*) AS rejected_count
FROM silver.rejected_records
GROUP BY source_table, rejection_reason
ORDER BY rejected_count DESC, source_table ASC;
```

| source_table | rejection_reason | rejected_count | Penjelasan Aturan Kualitas & Validasi Tipe/Integritas |
|---|---|---:|---|
| `silver.web_events` | `duplicate_event_id` | 986 | Deduplikasi deterministik atas `event_id` duplikat pada stream web events |
| `silver.payment_events` | `duplicate_event_id` | 667 | Deduplikasi deterministik atas `event_id` duplikat pada payment events |
| `silver.refund_events` | `duplicate_event_id` | 139 | Deduplikasi deterministik atas `event_id` duplikat pada refund events |
| `silver.return_events` | `duplicate_event_id` | 113 | Deduplikasi deterministik atas `event_id` duplikat pada return events |
| `silver.support_events` | `duplicate_event_id` | 101 | Deduplikasi deterministik atas `event_id` duplikat pada support events |
| `silver.order_items` | `missing_fk_product_id` | 3 | Validasi FK: `product_id` pada order item tidak terdaftar di `silver.products` |
| `silver.inventory_snapshots` | `duplicate_inventory_snapshot` | 1 | Deduplikasi komposit `(product_id, location_id, snapshot_date)` |
| `silver.order_items` | `invalid_negative_or_zero_quantity` | 1 | Validasi tipe & domain integer: `quantity <= 0` ditolak sesuai aturan bisnis |
| `silver.orders` | `duplicate_order_id` | 1 | Deduplikasi `order_id` duplikat (`source_row_id` berpenanda `DUPLICATE`) |
| **TOTAL** | | **2,012** | **Seluruh 2.012 baris memiliki alasan penolakan eksplisit & payload JSONB asli** |

### 6.3 Rekonsiliasi Finansial, Kuantitas, dan Event `Silver` $\rightarrow$ `Gold`

**SQL Query Rekonsiliasi Lintas Tabel `Silver` & `Gold`:**
```sql
SELECT
    (SELECT COUNT(*) FROM silver.orders) AS silver_orders_count,
    (SELECT COUNT(*) FROM gold.order_360) AS gold_order_360_count,
    (SELECT SUM(order_count) FROM gold.customer_daily) AS gold_customer_daily_orders,
    (SELECT SUM(attributed_orders) FROM gold.channel_campaign_daily) AS gold_campaign_daily_orders,
    (SELECT SUM(total_orders) FROM gold.executive_kpis_daily) AS gold_exec_kpis_orders,
    (SELECT SUM(quantity * unit_price)::NUMERIC(14,2) FROM silver.order_items) AS silver_items_gmv,
    (SELECT SUM(gross_merchandise_value) FROM gold.order_360) AS gold_order_360_gmv,
    (SELECT SUM(gross_revenue) FROM gold.customer_daily) AS gold_customer_daily_gmv,
    (SELECT SUM(gross_revenue) FROM gold.product_daily) AS gold_product_daily_gmv,
    (SELECT SUM(gross_revenue) FROM gold.channel_campaign_daily) AS gold_campaign_daily_gmv,
    (SELECT SUM(gross_revenue) FROM gold.executive_kpis_daily) AS gold_exec_kpis_gmv,
    (SELECT SUM(amount)::NUMERIC(14,2) FROM silver.payment_events WHERE event_type = 'PAYMENT_CAPTURED') AS silver_captured_payments,
    (SELECT SUM(captured_payment_amount) FROM gold.order_360) AS gold_captured_payments,
    (SELECT SUM(amount)::NUMERIC(14,2) FROM (
        SELECT DISTINCT ON (refund_id) refund_id, amount
        FROM silver.refund_events
        WHERE event_type = 'REFUND_COMPLETED'
        ORDER BY refund_id, occurred_at_utc DESC, event_id ASC
    ) r) AS silver_completed_refunds,
    (SELECT SUM(refunded_amount) FROM gold.order_360) AS gold_order_360_refunds,
    (SELECT COUNT(DISTINCT order_id) FROM silver.return_events) AS silver_orders_with_return_events,
    (SELECT COUNT(*) FROM gold.order_360 WHERE return_status <> 'none') AS gold_order_360_returned_orders,
    (SELECT SUM(return_count) FROM gold.customer_daily) AS gold_customer_daily_returns,
    (SELECT SUM(spend_amount)::NUMERIC(14,2) FROM silver.campaign_spend) AS silver_campaign_spend,
    (SELECT SUM(campaign_spend) FROM gold.channel_campaign_daily) AS gold_campaign_spend;
```

**Hasil Rekonsiliasi Metrik Lintas Tabel (`Silver` vs `Gold`):**

| Dimensi Rekonsiliasi | Sumber `Silver` | Nilai `Silver` | Tabel Target `Gold` | Nilai `Gold` | Selisih (*Variance*) |
|---|---|---:|---|---:|---:|
| **Jumlah Resolved Orders** | `silver.orders` (`COUNT(*)`) | `10,000` | `gold.order_360` / `customer_daily` / `channel_campaign_daily` / `executive_kpis_daily` | `10,000` | **`0`** |
| **Total Units Terjual** | `silver.order_items` (`SUM(quantity)`) | `62,379` | `gold.order_360` / `customer_daily` / `product_daily` | `62,379` | **`0`** |
| **Total GMV / Gross Revenue** | `silver.order_items` (`SUM(quantity * unit_price)`) | `15,100,182.54` | `gold.order_360` / `customer_daily` / `product_daily` / `channel_campaign_daily` / `executive_kpis_daily` | `15,100,182.54` | **`0.00`** |
| **Total Captured Payment** | `silver.payment_events` (`PAYMENT_CAPTURED`) | `13,137,988.14` | `gold.order_360` (`SUM(captured_payment_amount)`) | `13,137,988.14` | **`0.00`** |
| **Total Completed Refund** | `silver.refund_events` (deduplicated `REFUND_COMPLETED`) | `1,549,240.21` | `gold.order_360` / `customer_daily` / `channel_campaign_daily` | `1,549,240.21` | **`0.00`** |
| **Total Net Revenue** | Formula `GMV - discount + shipping - refund` | `13,429,824.83` | `gold.order_360` / `customer_daily` / `channel_campaign_daily` / `executive_kpis_daily` | `13,429,824.83` | **`0.00`** |
| **Total Returned Orders** | `silver.return_events` (`COUNT(DISTINCT order_id)`) | `1,372` | `gold.order_360` (`return_status <> 'none'`) & `gold.customer_daily` (`SUM(return_count)`) | `1,372` | **`0`** |
| **Total Campaign Spend** | `silver.campaign_spend` (`SUM(spend_amount)`) | `231,441.30` | `gold.channel_campaign_daily` (`SUM(campaign_spend)`) | `231,441.30` | **`0.00`** |

### 6.4 Hasil Pemeriksaan Quality Gate & Validasi Tipe Data (`ops.quality_check_results`)

Seluruh **27 pemeriksaan kualitas data otomatis** (14 pemeriksaan di `run_quality_gate()` pada layer Silver dan 13 pemeriksaan di `validate_gold()` pada layer Gold, termasuk validasi tipe data kolom via `information_schema.columns`) lulus 100% (`passed = true`):

| No | check_name | check_category | target_table | expected_result | actual_result | passed |
|---:|---|---|---|---:|---:|:---:|
| 1 | `silver_orders_not_empty` | `business_rule` | `silver.orders` | `> 0` | `10000` | `true` |
| 2 | `silver_orders_unique_order_id` | `uniqueness` | `silver.orders` | `0` | `0` | `true` |
| 3 | `silver_payment_events_unique_event_id` | `uniqueness` | `silver.payment_events` | `0` | `0` | `true` |
| 4 | `silver_refund_events_unique_event_id` | `uniqueness` | `silver.refund_events` | `0` | `0` | `true` |
| 5 | `silver_order_items_positive_qty_and_valid_amounts` | `business_rule` | `silver.order_items` | `0` | `0` | `true` |
| 6 | `silver_orders_normalized_status_enum` | `business_rule` | `silver.orders` | `0` | `0` | `true` |
| 7 | `silver_scd2_valid_intervals` | `business_rule` | `silver.product_categories` | `0` | `0` | `true` |
| 8 | `silver_orders_fk_integrity` | `referential_integrity` | `silver.orders` | `0` | `0` | `true` |
| 9 | `silver_order_items_fk_integrity` | `referential_integrity` | `silver.order_items` | `0` | `0` | `true` |
| 10 | `silver_events_fk_orders_integrity` | `referential_integrity` | `silver.payment_events` | `0` | `0` | `true` |
| 11 | `silver_orders_bronze_reconciliation` | `reconciliation` | `silver.orders` | `0` | `0` | `true` |
| 12 | `silver_all_tables_bronze_reconciliation` | `reconciliation` | `silver.rejected_records` | `0` | `0` | `true` |
| 13 | `silver_column_data_types_valid` | `business_rule` | `silver.orders` | `0` | `0` | `true` |
| 14 | `silver_rejected_records_logged` | `business_rule` | `silver.rejected_records` | `> 0` | `2012` | `true` |
| 15 | `gold_all_tables_populated` | `business_rule` | `gold.order_360` | `> 0` | `90` | `true` |
| 16 | `gold_order_360_grain_matches_silver_orders` | `reconciliation` | `gold.order_360` | `0` | `0` | `true` |
| 17 | `gold_order_360_net_revenue_identity` | `business_rule` | `gold.order_360` | `0` | `0` | `true` |
| 18 | `gold_order_360_payment_reconciliation` | `reconciliation` | `gold.order_360` | `0.00` | `0.00` | `true` |
| 19 | `gold_order_360_refund_reconciliation` | `reconciliation` | `gold.order_360` | `0.00` | `0.00` | `true` |
| 20 | `gold_customer_daily_revenue_and_order_reconciliation` | `reconciliation` | `gold.customer_daily` | `0.00` | `0.00` | `true` |
| 21 | `gold_product_daily_gmv_and_units_reconciliation` | `reconciliation` | `gold.product_daily` | `0.00` | `0.00` | `true` |
| 22 | `gold_channel_campaign_daily_spend_and_revenue_reconciliation` | `reconciliation` | `gold.channel_campaign_daily` | `0.00` | `0.00` | `true` |
| 23 | `gold_executive_kpis_daily_reconciliation` | `reconciliation` | `gold.executive_kpis_daily` | `0.00` | `0.00` | `true` |
| 24 | `gold_executive_kpis_rates_valid_bounds` | `business_rule` | `gold.executive_kpis_daily` | `0` | `0` | `true` |
| 25 | `gold_order_360_return_reconciliation` | `reconciliation` | `gold.order_360` | `0` | `0` | `true` |
| 26 | `gold_executive_kpis_refund_and_return_rate_formula` | `business_rule` | `gold.executive_kpis_daily` | `0` | `0` | `true` |
| 27 | `gold_contract_column_types_valid` | `business_rule` | `gold.order_360` | `0` | `0` | `true` |

---

## 7. Bukti Eksekusi dari Database Kosong & Kesamaan Hasil Rerun (*Idempotency Proof*)

Untuk membuktikan bahwa pipeline berjalan mulus dari **database kosong (`0` tabel)** dan bersifat **100% idempoten** saat dijalankan ulang (*rerun*), urutan lengkap task DAG (`validate_raw_data` $\rightarrow$ `create_*_schema` $\rightarrow$ `build_bronze_layer` $\rightarrow$ `build_silver_layer` $\rightarrow$ `quality_gate` $\rightarrow$ `build_gold_layer` $\rightarrow$ `validate_gold`) dieksekusi dua kali berturut-turut:
1. **Run 1 (`run_id = 'initial_empty_db_001'`):** Dijalankan setelah seluruh skema (`bronze`, `silver`, `gold`, `ops`) dihapus (`0` tabel di database).
2. **Run 2 (`run_id = 'rerun_verify_002'`):** Dijalankan langsung di atas database hasil Run 1 tanpa menghapus tabel atau data terlebih dahulu.

### 7.1 Perbandingan Metadata Eksekusi pada `ops.pipeline_runs`

**SQL Query:**
```sql
SELECT
    run_id,
    status,
    manifest_checksum,
    bronze_record_count,
    silver_record_count,
    gold_record_count,
    rejected_record_count
FROM ops.pipeline_runs
WHERE run_id IN ('initial_empty_db_001', 'rerun_verify_002')
ORDER BY started_at ASC;
```

| run_id | Kondisi Awal Database | status | manifest_checksum | bronze_record_count | silver_record_count | gold_record_count | rejected_record_count |
|---|---|---|---:|---:|---:|---:|---:|
| **`initial_empty_db_001`** | Database Kosong (`0` tabel) | `SUCCESS` | `1349fb47b915...5468` | **158,771** | **156,759** | **29,750** | **2,012** |
| **`rerun_verify_002`** | Rerun di Atas DB Terisi | `SUCCESS` | `1349fb47b915...5468` | **158,771** | **156,759** | **29,750** | **2,012** |
| **Selisih (*Variance*)** | | **Identik** | **Identik** | **0** | **0** | **0** | **0** |

### 7.2 Perbandingan Baris, Metrik Finansial, Rate, dan Checksum MD5 Isi Tabel (Run 1 vs. Run 2)

| Tabel / Metrik | Hasil Run 1 (`initial_empty_db_001`) | Hasil Run 2 (`rerun_verify_002`) | Selisih (*Delta*) | Status Idempotensi |
|---|---:|---:|---:|:---:|
| **`gold.order_360` — Row Count** | `10,000` | `10,000` | `0` | **IDENTIK** |
| **`gold.order_360` — Total GMV** | `15,100,182.54` | `15,100,182.54` | `0.00` | **IDENTIK** |
| **`gold.order_360` — Total Discount** | `166,015.00` | `166,015.00` | `0.00` | **IDENTIK** |
| **`gold.order_360` — Total Shipping Revenue** | `44,897.50` | `44,897.50` | `0.00` | **IDENTIK** |
| **`gold.order_360` — Total Captured Payment** | `13,137,988.14` | `13,137,988.14` | `0.00` | **IDENTIK** |
| **`gold.order_360` — Total Refunded Amount** | `1,549,240.21` | `1,549,240.21` | `0.00` | **IDENTIK** |
| **`gold.order_360` — Total Net Revenue** | `13,429,824.83` | `13,429,824.83` | `0.00` | **IDENTIK** |
| **`gold.order_360` — Contract Refund Rate** | `0.2269` (`1,991 / 8,776`) | `0.2269` (`1,991 / 8,776`) | `0.0000` | **IDENTIK** |
| **`gold.order_360` — Contract Return Rate** | `0.1372` (`1,372 / 10,000`) | `0.1372` (`1,372 / 10,000`) | `0.0000` | **IDENTIK** |
| **`gold.order_360` — Content MD5 Hash** | `bfbfc366862ce4d0c3a0aad6275f9460` | `bfbfc366862ce4d0c3a0aad6275f9460` | `0` | **IDENTIK** |
| **`gold.customer_daily` — Row Count & Net Revenue** | `9,784` rows / `13,429,824.83` | `9,784` rows / `13,429,824.83` | `0` / `0.00` | **IDENTIK** |
| **`gold.customer_daily` — Content MD5 Hash** | `8997c95471f1e0a80059d3b68eecc7cf` | `8997c95471f1e0a80059d3b68eecc7cf` | `0` | **IDENTIK** |
| **`gold.product_daily` — Row Count & Units Sold** | `7,200` rows / `62,379` units | `7,200` rows / `62,379` units | `0` / `0` | **IDENTIK** |
| **`gold.product_daily` — Refunded Units & Stockout Days** | `7,478` units / `416` days | `7,478` units / `416` days | `0` / `0` | **IDENTIK** |
| **`gold.product_daily` — Content MD5 Hash** | `913d185f8a25985951ea6ec53c6720ff` | `913d185f8a25985951ea6ec53c6720ff` | `0` | **IDENTIK** |
| **`gold.channel_campaign_daily` — Row Count & Spend** | `2,676` rows / `231,441.30` | `2,676` rows / `231,441.30` | `0` / `0.00` | **IDENTIK** |
| **`gold.channel_campaign_daily` — Content MD5 Hash** | `3cf9ce0bad77b69d1bde6d0001f90a7d` | `3cf9ce0bad77b69d1bde6d0001f90a7d` | `0` | **IDENTIK** |
| **`gold.executive_kpis_daily` — Row Count & Avg Rates** | `90` rows (`refund=0.2269`, `return=0.1371`, `repeat=0.7544`, `stockout=0.0578`, `support=0.1619`) | `90` rows (`refund=0.2269`, `return=0.1371`, `repeat=0.7544`, `stockout=0.0578`, `support=0.1619`) | `0.0000` | **IDENTIK** |
| **`gold.executive_kpis_daily` — Content MD5 Hash** | `2b9cacae9bd77a49981d43b73f54172c` | `2b9cacae9bd77a49981d43b73f54172c` | `0` | **IDENTIK** |
| **Silver Tables (`orders`, `items`, `events`, `rejected_records`)** | `156,759` loaded / `2,012` rejected | `156,759` loaded / `2,012` rejected | `0` | **IDENTIK** |

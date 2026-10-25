# Sample Engine Queries & Business Insights (Processed Gold Data)

Dokumen ini menyajikan hasil eksekusi query analitik dari layer **Gold PostgreSQL** (`gold.order_360`, `gold.customer_daily`, `gold.product_daily`, `gold.channel_campaign_daily`, dan `gold.executive_kpis_daily`) setelah seluruh pipeline ETL (**Bronze $\rightarrow$ Silver $\rightarrow$ Quality Gate $\rightarrow$ Gold $\rightarrow$ Validate Gold**) selesai memproses **10.000 resolved orders** selama periode **22 Juni 2026 – 19 September 2026 (90 hari bisnis)**.

---

## 1. Performa Pendapatan, AOV, dan Refund Rate per Sales Channel (`gold.order_360`)

**Pertanyaan Analitik:** *What is the total net revenue, average order value, and order refund rate by sales channel?*

**SQL Query:**
```sql
SELECT
    sales_channel,
    COUNT(*) AS total_orders,
    SUM(gross_merchandise_value) AS gmv,
    SUM(discount_amount) AS discounts,
    SUM(shipping_revenue) AS shipping,
    SUM(captured_payment_amount) AS captured_payment,
    SUM(refunded_amount) AS refunded_amount,
    SUM(net_revenue) AS net_revenue,
    ROUND(AVG(net_revenue), 2) AS avg_order_value,
    ROUND(AVG(CASE WHEN refunded_amount > 0 THEN 1.0 ELSE 0.0 END), 4) AS order_refund_rate
FROM gold.order_360
GROUP BY sales_channel
ORDER BY net_revenue DESC;
```

**Hasil Query (Data Aktual Terproses):**

| sales_channel | total_orders | gmv | discounts | shipping | captured_payment | refunded_amount | net_revenue | avg_order_value | order_refund_rate |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **WEB** | 2,507 | 3,885,039.33 | 41,547.00 | 11,203.00 | 3,362,861.94 | 413,887.11 | **3,440,808.22** | **1,372.48** | 0.2042 (20.42%) |
| **STORE** | 2,517 | 3,747,674.80 | 41,794.50 | 11,403.50 | 3,270,055.79 | 363,409.34 | **3,353,874.46** | **1,332.49** | 0.1947 (19.47%) |
| **MOBILE_APP** | 2,494 | 3,748,013.32 | 41,068.00 | 11,032.00 | 3,279,784.78 | 377,163.93 | **3,340,813.39** | **1,339.54** | 0.2005 (20.05%) |
| **MARKETPLACE** | 2,482 | 3,719,455.09 | 41,605.50 | 11,259.00 | 3,225,285.63 | 394,779.83 | **3,294,328.76** | **1,327.29** | 0.1970 (19.70%) |
| **TOTAL** | **10,000** | **15,100,182.54** | **166,015.00** | **44,897.50** | **13,137,988.14** | **1,549,240.21** | **13,429,824.83** | **1,342.98** | **0.1991 (19.91%)** |

**Insight Bisnis:**
- **Distribusi Omnichannel yang Seimbang:** Volume pesanan terdistribusi sangat merata di keempat kanal (~2.482 hingga 2.517 pesanan per kanal), namun kanal **WEB** memimpin perolehan *Net Revenue* sebesar **3,44 juta** dan memiliki *Average Order Value (AOV)* tertinggi (**1.372,48** per pesanan).
- **Kualitas Transaksi Toko Fisik (`STORE`):** Meskipun kanal `WEB` menghasilkan GMV tertinggi, kanal **`STORE`** mencatat tingkat *refund* terendah (**19,47%** dengan nilai *refund* **363.409,34**, lebih rendah ~50,4 ribu dibanding `WEB`). Hal ini menunjukkan pelanggan yang berbelanja langsung di toko fisik memiliki ekspektasi produk yang lebih akurat sehingga meminimalkan pengembalian dana.

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

*(Catatan: Selain 10 campaign berbayar di atas, terdapat **2.517 pesanan organik `STORE` (`UNATTRIBUTED`)** dengan `campaign_spend = 0.00` dan `net_revenue = 3,353,874.46` sehingga total atribusi order merekonsiliasi tepat 10.000 pesanan).*

**Insight Bisnis:**
- **Efisiensi Biaya Iklan Terbaik (`CMP-04` dan `CMP-05`):** Campaign **`CMP-04`** menghasilkan **ROAS tertinggi (46,54x)** diikuti **`CMP-05` (45,89x)** yang juga mencetak volume pesanan tertinggi (**783 pesanan**). Sebaliknya, **`CMP-10`** menyerap anggaran iklan terbesar (**25.043,21**) namun menghasilkan ROAS paling rendah (**39,27x**).
- **Rekomendasi Alokasi Budget:** Tim marketing dapat merealokasikan 15–20% anggaran dari `CMP-10` dan `CMP-03` ke `CMP-04` dan `CMP-09` (yang memiliki *net revenue* tertinggi sebesar **1.047.305,58** dan *refund* rendah sebesar **100.830,80**).

---

## 3. Performa Kategori Produk, Refund Impact, dan Risiko Stockout (`gold.product_daily`)

**Pertanyaan Analitik:** *Which product categories generated the most revenue, highest refund impact, and highest stockout rates (using point-in-time SCD2 categories)?*

**SQL Query:**
```sql
SELECT
    active_category,
    COUNT(DISTINCT product_id) AS distinct_products,
    SUM(units_sold) AS units_sold,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_revenue) AS net_revenue,
    SUM(gross_revenue - net_revenue) AS discount_and_refund_impact,
    SUM(refunded_units) AS refunded_units,
    COUNT(*) FILTER (WHERE stockout_flag IS TRUE) AS stockout_product_days,
    ROUND(AVG(stockout_flag::INT) FILTER (WHERE stockout_flag IS NOT NULL), 4) AS stockout_rate
FROM gold.product_daily
GROUP BY active_category
ORDER BY net_revenue DESC;
```

**Hasil Query (Data Aktual Terproses):**

| active_category | distinct_products | units_sold | gross_revenue | net_revenue | discount_and_refund_impact | refunded_units | stockout_product_days | stockout_rate |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| **fashion** | 17 | 13,195 | 3,308,526.87 | **2,916,761.77** | **391,765.10** | 3,715 | 85 | 0.0570 (5.70%) |
| **electronics** | 18 | 13,360 | 3,128,788.43 | **2,777,836.29** | 350,952.14 | 3,791 | 81 | 0.0526 (5.26%) |
| **sports** | 19 | 13,967 | 2,992,623.34 | **2,654,823.41** | 337,799.93 | **3,903** | **91** | 0.0562 (5.62%) |
| **home** | 13 | 8,518 | 2,264,245.00 | **2,010,010.64** | 254,234.36 | 2,298 | 69 | **0.0704 (7.04%)** |
| **grocery** | 12 | 8,360 | 2,133,670.33 | **1,883,063.09** | 250,607.24 | 2,328 | 54 | 0.0551 (5.51%) |
| **beauty** | 7 | 4,979 | 1,272,328.57 | **1,142,447.10** | 129,881.47 | 1,325 | 36 | 0.0610 (6.10%) |

**Insight Bisnis:**
- **Kontributor Utama Pendapatan vs. Erosi Refund:** Kategori **`fashion`** (**2,92 juta** *net revenue*), **`electronics`** (**2,78 juta**), dan **`sports`** (**2,65 juta**) menyumbang **61,9%** dari total pendapatan bersih produk. Namun, **`fashion`** juga mengalami selisih *gross-to-net* (dampak diskon + *refund*) terbesar yaitu **391.765,10** (11,84% dari *gross revenue*), yang lazim terjadi akibat ketidakcocokan ukuran/model pakaian.
- **Bottleneck Inventory pada Kategori `home` dan `sports`:** Kategori **`home`** memiliki *stockout rate* tertinggi (**7,04%** dari hari produk aktif mengalami kehabisan stok di minimal satu lokasi toko), sedangkan **`sports`** mencatat jumlah kejadian *stockout* tertinggi (**91 *product-days***) sekaligus penjualan unit tertinggi (**13.967 unit**). Penambahan *safety stock* pada kategori `home` dan `sports` berpotensi langsung meningkatkan konversi penjualan.

---

## 4. Retensi Pelanggan, Repeat Customer Rate, & Beban Customer Support (`gold.customer_daily`)

**Pertanyaan Analitik:** *How do customer retention (repeat customer count) and customer support contacts vary across active channels, and who are the top revenue-generating customers?*

**SQL Query (Per Kanal Aktif):**
```sql
SELECT
    active_channel,
    COUNT(*) AS customer_days,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(DISTINCT customer_id) FILTER (WHERE repeat_customer_flag) AS repeat_customers,
    SUM(order_count) AS total_orders,
    SUM(net_revenue) AS net_revenue,
    SUM(refund_amount) AS refund_amount,
    SUM(return_count) AS return_count,
    SUM(support_contacts) AS support_contacts
FROM gold.customer_daily
GROUP BY active_channel
ORDER BY net_revenue DESC;
```

**Hasil Query (Data Aktual Terproses):**

| active_channel | customer_days | unique_customers | repeat_customers | total_orders | net_revenue | refund_amount | return_count | support_contacts |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| **WEB** | 2,455 | 1,610 | 1,294 | 2,502 | 3,421,821.22 | 422,132.73 | 536 | 387 |
| **STORE** | 2,464 | 1,567 | **1,311** | 2,513 | 3,361,502.70 | 358,821.24 | 490 | **434** |
| **MOBILE_APP** | 2,445 | 1,587 | 1,266 | 2,501 | 3,347,190.22 | 373,671.06 | 503 | 407 |
| **MARKETPLACE** | 2,420 | 1,569 | 1,294 | 2,484 | 3,299,310.69 | 394,615.18 | 516 | 391 |

**Insight Bisnis:**
- **Loyalitas Pelanggan Lintas Kanal yang Tinggi:** Dari 2.500 pelanggan terdaftar, setiap kanal memiliki lebih dari **1.266–1.311 pelanggan berulang (*repeat customers*)**, dengan toko fisik (**`STORE`**) memimpin jumlah *repeat customer* tertinggi (**1.311 pelanggan unik**).
- **Korelasi Tiket Support dengan Return/Refund:** Total terdapat **1.619 tiket bantuan pelanggan (`support_contacts`)** dari 1.187 pelanggan unik. Pelanggan bernilai tinggi (seperti `CUST-00829` dengan *net revenue* **19.479,34** dari 8 pesanan dan `CUST-02347` dengan *net revenue* **19.321,62** dari 12 pesanan) rata-rata menghubungi *support* 3–4 kali, sehingga penyediaan layanan *priority support* untuk segmen VIP sangat krusial untuk menjaga retensi.

---

## 5. Tren Executive KPIs Harian (`gold.executive_kpis_daily`)

**Pertanyaan Analitik:** *What are the overall 90-day executive KPI averages and daily trend highlights across the business?*

**SQL Query:**
```sql
SELECT
    COUNT(*) AS total_days,
    MIN(metric_date) AS start_date,
    MAX(metric_date) AS end_date,
    SUM(total_orders) AS total_orders,
    SUM(gross_revenue) AS total_gross_revenue,
    SUM(net_revenue) AS total_net_revenue,
    ROUND(AVG(average_order_value), 2) AS avg_daily_aov,
    ROUND(AVG(refund_rate), 4) AS avg_daily_refund_rate,
    ROUND(AVG(return_rate), 4) AS avg_daily_return_rate,
    ROUND(AVG(repeat_customer_rate), 4) AS avg_daily_repeat_rate,
    ROUND(AVG(stockout_rate), 4) AS avg_daily_stockout_rate,
    ROUND(AVG(support_contact_rate), 4) AS avg_daily_support_rate
FROM gold.executive_kpis_daily;
```

**Hasil Query Ringkasan 90 Hari (22 Juni 2026 – 19 September 2026):**

| total_days | start_date | end_date | total_orders | total_gross_revenue | total_net_revenue | avg_daily_aov | avg_daily_refund_rate | avg_daily_return_rate | avg_daily_repeat_rate | avg_daily_stockout_rate | avg_daily_support_rate |
|---:|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| **90** | 2026-06-22 | 2026-09-19 | **10,000** | **15,100,182.54** | **13,429,824.83** | **1,342.95** | **0.2269 (22.69%)** | **0.2041 (20.41%)** | **0.7544 (75.44%)** | **0.0578 (5.78%)** | **0.1619 (16.19%)** |

**Sampel 10 Hari Pertama (`gold.executive_kpis_daily`):**

| metric_date | total_orders | gross_revenue | net_revenue | average_order_value | refund_rate | return_rate | repeat_customer_rate | stockout_rate | active_customers | support_contact_rate |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 2026-06-22 | 124 | 167,534.15 | 148,586.40 | 1,198.28 | 0.2182 | 0.2581 | 0.0248 | 0.0875 | 121 | 0.1532 |
| 2026-06-23 | 106 | 174,361.40 | 150,208.63 | 1,417.06 | 0.3333 | 0.2170 | 0.0792 | 0.0625 | 101 | 0.1226 |
| 2026-06-24 | 110 | 143,363.09 | 128,670.56 | 1,169.73 | 0.2737 | 0.1818 | 0.1204 | 0.0625 | 108 | 0.1818 |
| 2026-06-25 | 96 | 137,301.01 | 121,962.98 | 1,270.45 | 0.2151 | 0.1354 | 0.1489 | 0.0750 | 94 | 0.1771 |
| 2026-06-26 | 106 | 158,647.14 | 139,799.40 | 1,318.86 | 0.2556 | 0.1887 | 0.1635 | 0.0500 | 104 | 0.1038 |
| 2026-06-27 | 102 | 151,393.00 | 139,188.94 | 1,364.60 | 0.1591 | 0.2157 | 0.1818 | 0.0625 | 99 | 0.1569 |
| 2026-06-28 | 110 | 176,426.14 | 162,951.40 | 1,481.38 | 0.1895 | 0.2091 | 0.2243 | 0.0875 | 107 | 0.1636 |
| 2026-06-29 | 106 | 142,647.57 | 122,471.23 | 1,155.39 | 0.2660 | 0.1981 | 0.3302 | 0.0375 | 106 | 0.1226 |
| 2026-06-30 | 101 | 152,426.99 | 136,702.86 | 1,353.49 | 0.2442 | 0.1881 | 0.3700 | 0.0500 | 100 | 0.1683 |
| 2026-07-01 | 108 | 152,779.80 | 135,133.72 | 1,251.24 | 0.1957 | 0.1759 | 0.3398 | 0.0625 | 103 | 0.1667 |

**Insight Bisnis:**
- **Pertumbuhan Pesat Repeat Customer Rate:** Pada hari pertama observasi (`2026-06-22`), *repeat customer rate* baru sebesar **2,48%** (dari pelanggan yang berbelanja lebih dari satu kali pada hari yang sama), kemudian tumbuh konsisten melampaui **37,00%** pada hari ke-9 dan mencapai rata-rata **75,44%** sepanjang periode 90 hari. Hal ini mengonfirmasi bahwa pertumbuhan pendapatan perusahaan ditopang secara kuat oleh pembelian berulang (*customer retention*).
- **Pengendalian Refund & Return Rate:** Rata-rata *refund rate* harian terhadap pesanan yang pembayarannya berhasil di-*capture* adalah **22,69%**, sedangkan *return rate* terhadap total pesanan adalah **20,41%** (dengan **527 pesanan** berstatus *fully refunded* di mana `net_revenue = 0.00`). Rekonsiliasi ketat di layer Silver dan Gold memastikan tidak ada *double-counting* antara event `REFUND_ISSUED` dan `REFUND_COMPLETED`, maupun *split payment* pada `PAYMENT_CAPTURED`.

Insight Bisnis yang diperoleh

Query di PGAdmin4:
Distribusi Volume Transaksi yang Merata
Hal pertama yang langsung terlihat adalah volume pesanan (total_orders) yang tersebar hampir setara di setiap saluran
Berkisar antara 2.482 hingga 2.517 transaksi. Toko fisik (STORE) memimpin sedikit dengan 2.517 pesanan, disusul oleh kanal Web dengan 2.507 pesanan.
Keseimbangan ini mengindikasikan bahwa bisnis berhasil membangun kehadiran omnichannel 
Di mana pelanggan aktif berbelanja melalui berbagai platform tanpa ada ketergantungan ekstrem pada satu titik sentuh saja

Airflow:
Tampilan grafis Apache Airflow ini menyajikan struktur alur kerja (DAG Graph View) dari pipeline omnichannel_retail_pipeline. Jika dibaca dari sudut pandang bisnis dan tata kelola data perusahaan, arsitektur alur ini memberikan beberapa insight penting:

1. Kesiapan Data Bisnis untuk Pengambilan Keputusan 
Pipeline ini secara eksplisit mengalirkan data mentah menuju build_gold_layer melalui tahapan Bronze dan Silver:
Di level bisnis ritel, Gold Layer adalah tempat berkumpulnya metrik-metrik finansial tingkat eksekutif—seperti perhitungan GMV, Net Revenue, AOV, dan Refund Rate per kanal penjualan. Seluruh rangkaian proses berhasil selesai dengan status hijau (success), menandakan bahwa data transaksi di layer analitik bisnis sudah terisi secara utuh, konsisten, dan siap dikonsumsi langsung oleh tim manajemen bisnis (C-level/decision makers). 

2. Kehadiran Skema Operasional Khusus (Auditing & Traceability)
Adanya task create_ops_schema yang berdampingan dengan skema data analitik menunjukkan kesadaran tinggi akan tata kelola kepatuhan (governance & auditability):
Skema ops biasanya berfungsi mencatat metadata teknis seperti log eksekusi, jejak audit, waktu pemrosesan, dan jejak lineage data.   Bagi bisnis, ini memberikan kepastian bahwa setiap data rupiah/omzet yang dilaporkan ke pemangku kepentingan memiliki rekam jejak yang transparan dan dapat diaudit sewaktu-waktu jika terjadi selisih rekonsiliasi keuangan.

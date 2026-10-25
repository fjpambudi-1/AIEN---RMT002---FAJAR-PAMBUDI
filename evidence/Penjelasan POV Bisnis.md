Insight Bisnis yang diperoleh

Query di PGAdmin4:
Distribusi Volume Transaksi yang Merata
Hal pertama yang langsung terlihat adalah volume pesanan (total_orders) yang tersebar hampir setara di setiap saluran
Berkisar antara 2.482 hingga 2.517 transaksi. Toko fisik (STORE) memimpin sedikit dengan 2.517 pesanan, disusul oleh kanal Web dengan 2.507 pesanan.
Keseimbangan ini mengindikasikan bahwa bisnis berhasil membangun kehadiran omnichannel 
Di mana pelanggan aktif berbelanja melalui berbagai platform tanpa ada ketergantungan ekstrem pada satu titik sentuh saja

Airflow:
Alur tugas bergerak secara berurutan mulai dari inisialisasi dan validasi (validate_manifest, init_schemas), 
penyerapan data mentah (load_bronze), 
pembersihan dan transformasi (build_silver), 
hingga agregasi data siap pakai untuk analitik bisnis (build_gold)

Ini menjelaskan asal-usul tabel gold.order_360 pada analisis SQL sebelumnya: 
data tersebut dihasilkan langsung melalui tahapan build_gold dalam pipeline ini setelah melewati penyaringan bertahap

Pipeline ini tidak sekadar memindahkan data, melainkan menyematkan kontrol kualitas aktif di setiap tahap penting:
Terdapat tahapan eksplisit quality_gate (sebelum masuk ke Gold) dan validate_gold (setelah pembentukan data analitik)
Kedua run yang tercatat menunjukkan status hijau (success) pada seluruh langkah validasi ini, 
yang berarti data agregat ritel yang dihasilkan telah memenuhi kriteria integritas, schema check, maupun batasan kualitas tanpa adanya data korup.

Task terakhir bernama record_failure berstatus merah muda (skipped) pada kedua proses eksekusi.
Ini merupakan tanda positif dalam pola perancangan DAG: 
task record_failure kemungkinan dikonfigurasi dengan TriggerRule.ONE_FAILED atau 
serupa untuk mencatat insiden hanya jika ada task hulu yang gagal. 
Status skipped memastikan bahwa pipeline berjalan mulus tanpa interupsi kesalahan sama sekali.

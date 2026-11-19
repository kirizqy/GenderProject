# **GENDER PAY GAP ANALYSIS**
### **Resource**: 
[Eurostat](https://ec.europa.eu/eurostat/databrowser/explore/all/tb_eu?sort=category&lang=en&subtheme=sdg.sdg_05&display=list)
### Team: 
**Data Engineer**: Khoizuron Syakira

**Database & Data Modeling**: Muhammad Rizqy Fadillah

**Data Analyst**: Guntur Rizkiyana Muhammad
## **Background**
Kesetaraan gender merupakan pilar utama agenda pembangunan berkelanjutan Uni Eropa (SDG 5), namun kesenjangan upah antar-gender (Gender Pay Gap) masih menjadi tantangan di berbagai sektor industri. Analisis ini menggunakan data resmi Eurostat (2015–2024) untuk memetakan disparitas upah lintas sektor serta menguji pengaruh tingkat pendidikan tinggi, partisipasi kerja, dan kepemimpinan manajerial perempuan guna menghasilkan rekomendasi kebijakan berbasis data.

## **Objective**
Mengevaluasi tren Gender Pay Gap di Eropa (2015–2024) serta faktor-faktor yang dapat menurunkannya untuk memberikan rekomendasi kebijakan yang efektif.

## Stakeholder

Proyek ini melibatkan berbagai stakeholder yang memiliki peran dalam dalam meregulasi dan menekan Gender Pay Gap

| Stakeholder | Peran |
|---|---|
| **Pembuat Kebijakan & Regulator Uni Eropa** | Merumuskan regulasi transparansi upah lintas industri (seperti EU Pay Transparency Directive), mengalokasikan anggaran beasiswa pendidikan tinggi bagi perempuan, serta merancang insentif pajak/regulasi bagi perusahaan yang meningkatkan kuota kepemimpinan perempuan | 
| **Pimpinan Perusahaan** | Mengevaluasi struktur kompensasi (gaji pokok, bonus, dan promosi), menghilangkan bias gender dalam jenjang karier, serta membuka akses lebih luas bagi perempuan ke posisi manajerial. | 
| **Institusi Pendidikan Tinggi & Lembaga Pelatihan** | Mendorong partisipasi mahasiswi di bidang-bidang strategis dan berpenghasilan tinggi (seperti keuangan, teknologi, dan manajemen bisnis). |
| **European Institute for Gender Equality / EIGE** | Mendapatkan transparansi informasi mengenai standar pengupahan di tiap sektor industri dan memperjuangkan kesetaraan hak serta peluang promosi di tempat kerja. | 


## **Data Engineering (Pipeline & Infrastructure)**
### Data Engineering Pipeline

Alur kerja *data engineering* dirancang untuk memproses data dari sumber hingga siap dianalisis melalui beberapa tahapan utama:

#### 1. Alur Proses Pipeline (ETL & Validation)
![Pipeline](https://github.com/dutarangga3107-max/CODA-020-Final_Project/blob/main/Schema/pipeline%20.png?raw=true)
* **Extraction**: Membaca dan mengumpulkan data mentah dari *website* CTDC tanpa membebani kinerja sistem sumber.
* **Data Validation**: Memastikan data mematuhi logika bisnis (misal: validasi usia dan format data yang sesuai).
* **Transformation**: 
  * Pembersihan data: Menangani nilai kosong (*NULL*) dan menghapus baris duplikat.
  * Pemformatan data: Mengubah data mentah menjadi format yang mudah dianalisis.
* **Loading**: Memindahkan data yang sudah bersih dan valid ke destinasi akhir di basis data Neon.

#### 2. Airflow Orchestration
* **DAG Structure**: Menggunakan 1 DAG (`FP_dag.py`) dengan 3 *task* linear: `extract` → `transform` → `load`.
* **Execution**: Eksekusi dijalankan via `BashOperator`, di mana tiap *task* berjalan sebagai *subprocess* Python independen.
* **Schedule**: Dijadwalkan berjalan setiap hari Sabtu, menit ke-10 hingga 30 (kelipatan 10) pada jam 09:00 pagi.
* **Retry Policy**: Konfigurasi otomatis 1 kali percobaan ulang (*retry*) dengan jeda 5 menit jika terjadi kegagalan pada *task*.

#### 3. Environment & Infrastructure
* **Docker Setup**: Menggunakan Airflow via *Docker Compose* dengan *custom image* `airflow-spark` (basis `apache/airflow:2.3.4` yang dilengkapi Java serta pustaka pendukung seperti *great_expectations*, *pandas*, *sqlalchemy*, *beautifulsoup4*, *pyarrow*, dll).
* **Storage Volume**: Folder direktori `data/`, `dags/`, `scripts/`, dan `logs/` di-mount sebagai *volume* lokal sehingga file *parquet* tetap aman meskipun *container* direstart.
### Database & Data Modeling 
Proses ini mengubah data statistik dari Eurostat mengenai kesenjangan gender di Eropa menjadi struktur basis data relasional yang terorganisasi. Data dari empat dataset, yaitu *Gender Pay Gap*, *Employment Gap*, *Female Education*, dan *Women in Senior Management*, dikelola menggunakan PostgreSQL.

Database dirancang menggunakan pendekatan *Dimensional Modeling* dengan memisahkan data pengukuran ke dalam *Fact Tables* dan informasi kategoris ke dalam *Dimension Tables*. Pemisahan ini bertujuan untuk mengurangi redundasi data, menjaga integritas relasional, serta memudahkan proses analisis dan visualisasi.
#### Database Architecture 
![Galaxy Schema](ERD_Gender_Pay_Gap_Analysis.png)
Arsitektur basis data dirancang menggunakan *Galaxy Schema*, yang terdiri dari:
* **Fact Tables**: Menyimpan metrik utama yang digunakan dalam analisis, meliputi:
  * `fact_gender_pay_gap`: Menyimpan persentase kesenjangan upah berdasarkan negara, tahun, dan industri.
  * `fact_employment_gap`: Menyimpan kesenjangan tingkat pekerjaan antara laki-laki dan perempuan berdasarkan negara, tahun, dan status pekerjaan.
  * `fact_female_education`: Menyimpan persentase perempuan berusia 25–34 tahun yang memiliki pendidikan tinggi.
  * `fact_women_management`: Menyimpan persentase perempuan yang menduduki posisi manajemen.
* **Dimension Tables**: Menyimpan informasi kategoris sebagai referensi untuk Fact Tables, meliputi:
  * `dim_country`: Kode dan nama negara.
  * `dim_year`: Tahun pengamatan selama periode 2015–2024.
  * `dim_industry`: Klasifikasi industri berdasarkan NACE Rev. 2.
  * `dim_employment_status`: Kategori status pekerjaan.
  * `dim_management_position`: Kategori posisi manajemen.
### Data Preparation
#### Data Type Formatting & Integrity
* **Data Type Formatting**: Menyesuaikan tipe data berdasarkan karakteristik masing-masing kolom. Kode negara dan kategori menggunakan `VARCHAR`, tahun menggunakan `SMALLINT`, sedangkan nilai persentase dan metrik numerik menggunakan `NUMERIC(8,3)`.

* **Primary Key**: Menggunakan *Natural Key* untuk Dimension Tables, seperti `country_code`, `year`, dan kode kategori. Fact Tables menggunakan *Composite Primary Key* berdasarkan kombinasi kolom yang membentuk identitas unik setiap observasi.

* **Foreign Key**: Menerapkan relasi antara Fact Tables dan Dimension Tables untuk menjaga konsistensi referensi data.

### SQL & Analytics
Untuk mendukung kebutuhan analisis dan menjawab lima pertanyaan penelitian (*Key Questions*), dirancang SQL Queries dan *SQL Views* yang menghubungkan Fact Tables dengan Dimension Tables.

Penggunaan *SQL Views* bertujuan menyederhanakan proses pengambilan data, mengurangi pengulangan sintaks `JOIN`, serta menyediakan struktur data yang lebih mudah digunakan oleh tim Analyst & Dashboard.

Beberapa kebutuhan analisis yang didukung meliputi:

1. **Gender Pay Gap Trend**: Menganalisis perkembangan kesenjangan upah berdasarkan industri selama 2015–2024.
2. **Industry Comparison**: Membandingkan tingkat kesenjangan upah antarindustri.
3. **Education and Pay Gap Relationship**: Menggabungkan data pendidikan perempuan dengan gender pay gap untuk analisis hubungan statistik.
4. **Employment Gap Analysis**: Mengidentifikasi kesenjangan tingkat pekerjaan antara laki-laki dan perempuan.
5. **Women in Management**: Menganalisis persentase perempuan dalam posisi manajemen.

Contoh implementasi SQL View untuk menampilkan detail Gender Pay Gap berdasarkan negara, tahun, dan industri:

```sql
CREATE OR REPLACE VIEW public.vw_gender_pay_gap_detail AS
SELECT
    c.country_code,
    c.country_name,
    y.year,
    i.industry_code,
    i.industry_name,
    f.pay_gap_percentage,
    f.observation_flag,
    f.confidentiality_status
FROM public.fact_gender_pay_gap f
JOIN public.dim_country c
    ON f.country_code = c.country_code
JOIN public.dim_year y
    ON f.year = y.year
JOIN public.dim_industry i
    ON f.industry_code = i.industry_code;
```
## **Data Dictionary**

#### A. COUNTRY

| Column | Data Type | Definisi |
|---|---|---|
| `country_code` | varchar(2) | Primary key yang menjadi identitas unik setiap negara berdasarkan kode Eurostat |
| `country_name` | varchar(100) | Nama lengkap negara berdasarkan kode negara Eurostat |

---

#### B. YEAR

| Column | Data Type | Definisi |
|---|---|---|
| `year` | SMALLINT | Primary key yang menyimpan tahun pengamatan dari 2015 sampai 2024 |

---

#### C. INDUSTRY

| Column | Data Type | Definisi |
|---|---|---|
| `industry_code` | varchar(5) | Primary key yang menjadi identitas unik setiap kategori industri berdasarkan kode NACE Rev. 2 |
| `industry_name` | varchar(200) | Nama atau deskripsi kategori industri berdasarkan klasifikasi NACE Rev. 2 |

---

#### D. EMPLOYMENT STATUS

| Column | Data Type | Definisi |
|---|---|---|
| `employment_status_code` | varchar(10) | Primary key yang menjadi identitas unik setiap kategori status pekerjaan berdasarkan kode Eurostat |
| `employment_status_name` | varchar(100) | Nama atau deskripsi kategori status pekerjaan, seperti employed persons, part-time workers, dan temporary workers |

---

#### E. MANAGEMENT POSITION

| Column | Data Type | Definisi |
|---|---|---|
| `management_position_code` | varchar(20) | Primary key yang menjadi identitas unik setiap kategori posisi manajemen berdasarkan kode Eurostat |
| `management_position_name` | varchar(150) | Nama atau deskripsi kategori posisi manajemen, seperti non-executive directors dan non-executives and executive directors |

---

#### F. GENDER PAY GAP

| Column | Data Type | Definisi |
|---|---|---|
| `country_code` | varchar(20) | Foreign Key yang menghubungkan data gender pay gap dengan tabel dim_country berdasarkan kode negara |
| `year` | SMALLINT | Foreign Key yang menghubungkan data gender pay gap dengan tabel dim_year berdasarkan tahun pengamatan |
| `industry_code` | varchar(5) | Foreign Key yang menghubungkan data gender pay gap dengan tabel dim_industry berdasarkan kode industri |
| `pay_gap_percentage` | numeric(8,3) | Menyimpan nilai persentase kesenjangan upah antara laki-laki dan perempuan pada suatu industri |
| `observation_flag` | varchar(10) | Menyimpan kode penanda atau status tambahan yang diberikan Eurostat pada suatu observasi |
| `confidentiality_status` | varchar(10) | Menunjukkan status kerahasiaan data berdasarkan kode yang diberikan Eurostat |

---

#### G. EMPLOYMENT GAP

| Column | Data Type | Definisi |
|---|---|---|
| `country_code` | varchar(20) | Foreign Key yang menghubungkan data employment gap dengan tabel dim_country berdasarkan kode negara |
| `year` | SMALLINT | Foreign Key yang menghubungkan data employment gap dengan tabel dim_year berdasarkan tahun pengamatan |
| `employment_status_code` | varchar(10) | Foreign Key yang menghubungkan data employment gap dengan tabel dim_employment_status berdasarkan kategori status pekerjaan |
| `employment_gap_percentage_points` | numeric(8,3) | Menyimpan nilai kesenjangan tingkat pekerjaan antara laki-laki dan perempuan dalam satuan percentage points |
| `observation_flag` | varchar(10) | Menyimpan kode penanda atau status tambahan yang diberikan Eurostat pada suatu observasi |

---

#### H. FEMALE EDUCATION

| Column | Data Type | Definisi |
|---|---|---|
| `country_code` | varchar(20) | Foreign Key yang menghubungkan data pendidikan perempuan dengan tabel dim_country berdasarkan kode negara |
| `year` | SMALLINT | Foreign Key yang menghubungkan data pendidikan perempuan dengan tabel year berdasarkan tahun pengamatan |
| `female_tertiary_education_percentage` | numeric(8,3) | Menyimpan persentase perempuan berusia 25–34 tahun yang telah menyelesaikan pendidikan tinggi berdasarkan klasifikasi ISCED 5–8 |
| `observation_flag` | varchar(10) | Menyimpan kode penanda atau status tambahan yang diberikan Eurostat pada suatu observasi |

---

#### I. WOMEN MANAGEMENT

| Column | Data Type | Definisi |
|---|---|---|
| `country_code` | varchar(20) | Foreign Key yang menghubungkan data perempuan dalam manajemen dengan tabel country berdasarkan kode negara |
| `year` | SMALLINT | Foreign Key yang menghubungkan data perempuan dalam manajemen dengan tabel year berdasarkan tahun pengamatan |
| `management_position_code` | varchar(20) | Foreign Key yang menghubungkan data perempuan dalam manajemen dengan tabel dim_management_position berdasarkan kategori posisi manajemen |
| `women_management_percentage` | numeric(8,3) | Menyimpan persentase perempuan yang menduduki posisi manajemen berdasarkan kategori posisi profesional |

## **Problem Statement**
"Meskipun tingkat pendidikan tinggi dan partisipasi kerja perempuan di kawasan Eropa terus meningkat selama periode 2015–2024, kesenjangan upah antar-gender (Gender Pay Gap) masih ada. Tanpa pemetaan empiris mengenai sektor yang paling terdampak serta pengujian statistik terhadap faktor-faktor pendorongnya—seperti tingkat pendidikan tinggi, selisih partisipasi kerja (employment gap), dan keterwakilan perempuan di posisi manajerial—pembuat kebijakan Uni Eropa berisiko merumuskan intervensi yang kurang tepat sasaran dalam mempercepat kesetaraan upah."


## **Key Questions**
1. Bagaimana tren gender pay gap di Eropa seiring waktu? 
2. Berdasarkan Industri, bagaimana perbandingan gender pay gap? Secara statistik, apakah perbedaan antar industri ini nyata atau hanya kebetulan?
3. Bagaimana trend tingkat edukasi wanita? Serta apa pengaruhnya terhadap gender pay gap?
4. Bagaimana tren persentase gap employment antara gender? Bagaimana pengaruhnya terhadap gender pay gap?
5. Bagaimana perkembangan tren persentase wanita masuk menjabat dalam manajemen suatu perusahaan? Bagaimana pengaruhnya terhadap gender pay gap?


## **Key Performance Indicators (KPI)**
**1. Exploitation Rate**<br>
**Explanation:** Mengukur persentase korban yang mengalami forced labour dari total korban.<br>
**Purpose:** Mengetahui proporsi keseluruhan korban yang mengalami eksploitasi.
  
**2. Forced Labour Rate**<br>
**Explanation:** Mengukur persentase korban yang mengalami forced labour dari total korban.<br>
**Purpose:** Mengidentifikasi seberapa sering kasus kerja paksa terjadi dalam dataset ini. 
  
**3. Sexual Exploitation Rate**<br>
**Explanation:** Mengukur persentase korban yang mengalami sexual exploitation dari total korban.<br>
**Purpose:** Mengetahui seberapa besar proporsi korban yang mengalami eksploitasi seksual.
  
**4. Debt Bondage Rate**<br>
**Explanation:** Mengukur persentase korban yang mengalami debt bondage dari total korban.<br>
**Purpose:** Mengidentifikasi tingkat eksploitasi yang berkaitan dengan keterikatan utang.
  
**5. Document Withholding Rate**<br>
**Explanation:** Mengukur persentase korban yang mengalami penahanan atau penyitaan dokumen dari total korban.<br>
**Purpose:** Mengetahui seberapa sering terjadi penahanan dokumen sebagao salah satu indikator resiko eksploitasi.
  
**6. High-Risk Sector Agriculture**<br>
**Explanation:** Mengukur exploitation rate khusus pada korban yang bekerja di sektor Agriculture.<br>
**Purpose:** Mengidentifikasi apakah sektor Agricukture merupakan sektor dengan resiko eksploitasi yang tinggi.
  
**7. High-Risk Age**<br>
**Explanation:** Menemukan kelompok usia dengan exploitation rate tertinggi.<br>
**Purpose:** Mengidentifikasi kelompok usia yang paling rentan terhadap eksploitasi.
  
**8. High-Risk Citizenship**<br>
**Explanation:** Mengukur exploitation rate berdasarkan kewarganegaraan korban.<br>
**Purpose:** Mengidentifikasi kelompok kewarganegaraan dengan tingkat resiko eksploitasi tertinggi.
  
**9. Recruitement Risk**<br>
**Explanation:** Mengukur exploitation rate berdasarkan recruitment relationship atau hubungan antara korban dan pihak yang merekrutnya.<br>
**Purpose:** Mengidentifikasi pola hubungan rekrutmen yang memiliki tingkat resiko eksploitasi lebih tinggi.

## **Findings**
**KQ1**. Risiko eksploitasi sangat dipengaruhi gender dan usia, perempuan (khususnya anak 09–17 tahun) dominan mengalami eksploitasi seksual, sementara laki-laki dominan mengalami kerja paksa, dengan pola yang juga bervariasi menurut kewarganegaraan.<br>
**KQ2.** Setiap sektor memiliki "signature" eksploitasi yang khas dan hampir murni satu jenis seperti sektor seks (prostitusi, pornografi) hampir eksklusif eksploitasi seksual, sektor kerja fisik (agriculture, construction) hampir eksklusif kerja paksa, kecuali hospitality yang menunjukkan risiko ganda.<br>
**KQ3.** Relasi personal yang dekat (intimate partner, family) justru berkorelasi dengan rate eksploitasi seksual tertinggi, sedangkan relasi non-personal ("other"/agen) lebih berkorelasi dengan kerja paksa  membalikkan asumsi umum bahwa orang asing adalah ancaman utama.<br>
**KQ4.** Anak perempuan yang direkrut melalui relasi dekat memiliki risiko eksploitasi seksual paling ekstrem, sementara anak laki-laki menunjukkan pola eksploitasi yang lebih campuran antara kerja paksa dan seksual.<br>
**KQ5.** Profil korban dengan risiko paling ekstrem adalah kombinasi anak usia sangat muda, sektor eksploitasi seksual, dan direkrut oleh keluarga sendiri seperti menunjukkan bahwa kombinasi faktor demografis, sektor, dan relasi recruiter jauh lebih prediktif dibanding faktor tunggal manapun.<br>
**KQ6**. Berdasarkan uji statistik Spearman ($\rho = 0,7204$; $p = 0,000106$), jumlah korban dengan hubungan rekrutmen Other menunjukkan tren peningkatan jangka panjang yang signifikan dengan puncak tertinggi pada 2016, sementara kategori Family, Friend, dan Intimate Partner masing-masing mencapai puncaknya pada 2016 atau 2019 sebelum mengalami penurunan hingga tahun 2024.

## **Conclusion**
Perkembangan Gender Pay Gap di Eropa selama 2015-2024 semakin membaik. Terjadi penurunan dari 13.51% menjadi 10.62% atau menurun sebesar 2.89%. Namun perlu diingat, tiap industri memiliki besaran Gender Pay Gap yang berbeda. Gender Employment Gap tidak memiliki pengaruh yang signifikan secara statistik dalam menurunkan Gender Pay Gap. Sehingga, kita bisa berfokus pada peningkatan persentase edukasi tingkat tinggi bagi wanita dan wanita dalam posisi manajerial untuk menurunkan Gender Pay Gap secara efektif.

## **Recommendation**
Untuk menurunkan Gender Pay Gap dengan efektif, pemerintah Uni Eropa dapat merumuskan kebijakan sebagai berikut:

1. Regulasi Upah Industri: Menerapkan standar pelaporan kesenjangan upah berkala pada industri guna mengatur dan menekan ketimpangan upah, bonus dan jenjang karier.
2. Perluasan Akses Pendidikan Tinggi: Memperluas beasiswa dan akses pendidikan tinggi bagi wanita.
3. Insentif Kepemimpinan Manajerial: Memberikan insentif bagi perusahaan yang meningkatkan persentase perempuan di jajaran manajerial.

### cara run
**Isi koneksi** : isi url_database_neon di FP_load.py, lalu copy file dan paste ke `airflow_spark/scripts/`
**selanjut nya bisa baca readme di folder airflow**

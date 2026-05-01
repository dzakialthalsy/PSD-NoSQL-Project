# 🎬 NoSQL Document Store — The Movies Dataset (5 CSV)

Project database NoSQL berbasis **MongoDB** menggunakan seluruh dataset
"The Movies Dataset" dari Kaggle, dikemas dalam **Docker** untuk Windows.

---

## 📁 Struktur Folder

```
movies-nosql/
│
├── data/                        ← 📂 LETAKKAN SEMUA FILE CSV DI SINI
│   ├── movies_metadata.csv
│   ├── credits.csv
│   ├── keywords.csv
│   ├── links.csv                ← NEW: External IDs (IMDb/TMDb/MovieLens)
│   └── ratings.csv              ← NEW: User ratings (~26 juta baris)
│
├── etl/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── etl.py                   ← ETL pipeline 6 langkah
│
├── mongo-init/
│   └── init.js
│
├── scripts/
│   └── contoh_query.py          ← 16 contoh query (semua data)
│
└── docker-compose.yml
```

---

## 🗄️ Struktur Dokumen Final (dalam MongoDB)

### Koleksi: `films`
Satu dokumen per film, berisi semua data yang telah di-denormalisasi:

```json
{
  "_id": "862",
  "title": "Toy Story",
  "release_date": "1995-10-30",
  "budget": 30000000,
  "revenue": 373554033,

  "tmdb_rating": {
    "average": 7.7,
    "count": 5415
  },

  "user_rating": {
    "avg": 3.9201,
    "count": 452,
    "std": 0.8341,
    "min": 0.5,
    "max": 5.0,
    "distribution": {
      "0.5": 3, "1.0": 8, "1.5": 5, "2.0": 18,
      "2.5": 28, "3.0": 79, "3.5": 89, "4.0": 131,
      "4.5": 52, "5.0": 39
    }
  },

  "external_ids": {
    "imdb_id": "tt0114709",
    "tmdb_id": "862",
    "movielens_id": 1
  },

  "genres": [{"id": 16, "name": "Animation"}, ...],
  "keywords": [{"id": 931, "name": "jealousy"}, ...],
  "cast": [{"name": "Tom Hanks", "character": "Woody"}, ...],
  "directors": [{"name": "John Lasseter", "id": 7879}],
  "production_companies": [...],
  "production_countries": [...],
  "spoken_languages": [...]
}
```

### Koleksi: `user_ratings`
Raw ratings dari pengguna MovieLens, disimpan terpisah (tidak di-embed):

```json
{
  "_id": "862",
  "ratings": [
    {"userId": 1, "rating": 4.0, "timestamp": 964982703},
    {"userId": 5, "rating": 4.0, "timestamp": 847434962},
    ...
  ]
}
```

> **Mengapa terpisah?** Satu film populer bisa punya ribuan rating.
> Meng-embed-nya ke dokumen film akan membuat dokumen terlalu besar
> dan memperlambat query yang tidak membutuhkan raw ratings.

---

## 🛠️ Prasyarat

| Software | Catatan |
|---|---|
| **Docker Desktop** | https://www.docker.com/products/docker-desktop |
| **RAM minimal 6GB** | Untuk Docker (ratings.csv sangat besar) |
| **Disk ~3GB** | Untuk dataset + MongoDB volume |

Di Docker Desktop → Settings → Resources, set Memory ke minimal **4GB**.

---

## 🚀 Cara Menjalankan

### Step 1 — Download Dataset Kaggle

1. Buka: https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset
2. Klik **Download** (butuh akun Kaggle gratis)
3. Extract ZIP → copy **semua 5 file CSV** ke folder `data/`

### Step 2 — Jalankan Docker

```powershell
# Di PowerShell, masuk ke folder project
cd C:\path\to\movies-nosql

# Build & jalankan semua service
docker compose up --build
```

ETL akan otomatis berjalan. Tunggu hingga muncul:
```
✅ ETL SELESAI! Semua 5 dataset telah diproses.
```

> ⏳ Estimasi waktu: **5–15 menit** tergantung spesifikasi PC
> (ratings.csv ~26 juta baris, proses terlama ada di langkah agregasi)

### Step 3 — Akses Database

| Akses | Alamat | Login |
|---|---|---|
| **Mongo Express** (Web UI) | http://localhost:8081 | admin / admin123 |
| **MongoDB Compass** | `mongodb://localhost:27017` | — |
| **mongosh** (CLI) | `docker exec -it movies_mongo mongosh` | — |

---

## 🔍 Menjalankan Contoh Query

```powershell
# Copy script ke container dan jalankan
docker cp scripts/contoh_query.py movies_etl:/app/
docker exec -e MONGO_URI=mongodb://mongo:27017/ movies_etl python /app/contoh_query.py
```

Script berisi **16 query** yang mencakup:
- Query dasar (genre, sutradara, full-text search)
- Query via IMDb ID / MovieLens ID (dari links.csv)
- Perbandingan TMDb rating vs User rating
- Histogram distribusi bintang per film
- Raw ratings per user dari koleksi `user_ratings`
- Agregasi: genre terbaik, film paling menguntungkan, hidden gems

---

## 💻 Query Cepat via mongosh

```powershell
docker exec -it movies_mongo mongosh movies_db
```

```javascript
// Total film
db.films.countDocuments()

// Cari via IMDb ID
db.films.findOne({ "external_ids.imdb_id": "tt0114709" })

// Top film berdasarkan user rating (min 100 voter)
db.films.find(
  { "user_rating.count": { $gte: 100 } },
  { title: 1, "user_rating.avg": 1, _id: 0 }
).sort({ "user_rating.avg": -1 }).limit(5)

// Distribusi rating Toy Story
db.films.findOne(
  { title: "Toy Story" },
  { "user_rating.distribution": 1, title: 1 }
)

// Raw ratings untuk film tertentu (dari koleksi user_ratings)
db.user_ratings.findOne({ _id: "862" })
```

---

## 🐳 Perintah Docker

```powershell
# Status container
docker compose ps

# Log ETL (pantau progress)
docker compose logs -f etl

# Stop semua
docker compose down

# Reset total (hapus data MongoDB)
docker compose down -v

# Jalankan ulang ETL saja
docker compose run --rm etl
```

---

## 🏗️ Arsitektur

```
  data/*.csv
      │
      ▼
  ┌───────────────────────────────────────┐
  │         ETL Pipeline (Python)         │
  │  1. Baca 5 CSV                        │
  │  2. Parse & bersihkan                 │
  │  3. Proses links → external_ids       │
  │  4. Proses ratings → agregasi + raw   │
  │  5. Merge & denormalisasi             │
  │  6. Import ke MongoDB                 │
  └───────────────┬───────────────────────┘
                  │
        ┌─────────▼──────────┐
        │     MongoDB        │
        │  movies_db         │
        │  ├─ films          │  ← ~45k dokumen lengkap
        │  └─ user_ratings   │  ← raw ratings per film
        └─────────┬──────────┘
                  │
        ┌─────────▼──────────┐
        │   Mongo Express    │  ← http://localhost:8081
        │   (Web UI)         │
        └────────────────────┘
```

---

## ⚖️ CAP Theorem

MongoDB default → **CP** (Consistency + Partition Tolerance)

| Properti | Status | Keterangan |
|---|---|---|
| **Consistency** | ✅ | Semua replica menampilkan data yang sama |
| **Availability** | ⚠️ | Mungkin ditolak saat ada node putus |
| **Partition Tolerance** | ✅ | Replica set otomatis pilih primary baru |

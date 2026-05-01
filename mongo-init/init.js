// Script inisialisasi MongoDB
// Dijalankan otomatis saat container pertama kali dibuat

db = db.getSiblingDB('movies_db');

// Buat user khusus untuk aplikasi
db.createUser({
  user: "movies_user",
  pwd: "movies_pass",
  roles: [{ role: "readWrite", db: "movies_db" }]
});

// Buat koleksi awal (akan diisi oleh ETL)
db.createCollection("films");

print("✅ MongoDB 'movies_db' siap digunakan.");

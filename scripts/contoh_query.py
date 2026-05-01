"""
contoh_query.py — Contoh Query MongoDB (mencakup semua 5 dataset)
Jalankan:
  docker cp scripts/contoh_query.py movies_etl:/app/
  docker exec -e MONGO_URI=mongodb://mongo:27017/ movies_etl python /app/contoh_query.py
"""

from pymongo import MongoClient
import json, os

MONGO_URI  = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME    = "movies_db"

client = MongoClient(MONGO_URI)
db     = client[DB_NAME]
films  = db["films"]
ur_col = db["user_ratings"]

SEP = "─" * 60

def show(label, result):
    print(f"\n{SEP}")
    print(f"  {label}")
    print(SEP)
    if isinstance(result, list):
        for doc in result:
            print(json.dumps(doc, indent=2, default=str))
    else:
        print(json.dumps(result, indent=2, default=str))

# ═══════════════════════════════════════════════════════════════
# BAGIAN 1: QUERY DASAR (movies_metadata + credits + keywords)
# ═══════════════════════════════════════════════════════════════

# 1. Cari satu film lengkap
q = films.find_one({"title": "Toy Story"})
show("Dokumen lengkap: Toy Story", q)

# 2. Top 5 film animasi berdasarkan TMDb rating
q = list(films.find(
    {"genres.name": "Animation"},
    {"title": 1, "tmdb_rating": 1, "release_date": 1, "_id": 0}
).sort("tmdb_rating.average", -1).limit(5))
show("Top 5 Animasi (TMDb Rating)", q)

# 3. Film oleh sutradara tertentu
q = list(films.find(
    {"directors.name": "Christopher Nolan"},
    {"title": 1, "release_date": 1, "tmdb_rating": 1, "_id": 0}
).sort("release_date", 1))
show("Film oleh Christopher Nolan", q)

# 4. Full-text search
q = list(films.find(
    {"$text": {"$search": "space adventure galaxy"}},
    {"title": 1, "score": {"$meta": "textScore"}, "_id": 0}
).sort([("score", {"$meta": "textScore"})]).limit(5))
show("Full-text search: 'space adventure galaxy'", q)

# ═══════════════════════════════════════════════════════════════
# BAGIAN 2: QUERY MENGGUNAKAN DATA LINKS (external_ids)
# ═══════════════════════════════════════════════════════════════

# 5. Cari film berdasarkan IMDb ID
q = films.find_one(
    {"external_ids.imdb_id": "tt0114709"},  # Toy Story
    {"title": 1, "external_ids": 1, "_id": 0}
)
show("Cari film via IMDb ID (tt0114709)", q)

# 6. Cari film berdasarkan MovieLens ID
q = films.find_one(
    {"external_ids.movielens_id": 1},
    {"title": 1, "external_ids": 1, "_id": 0}
)
show("Cari film via MovieLens ID (1)", q)

# 7. Film yang punya external link (IMDb + MovieLens)
q = list(films.find(
    {"external_ids": {"$ne": None}},
    {"title": 1, "external_ids": 1, "_id": 0}
).limit(3))
show("Contoh film dengan external_ids lengkap", q)

# ═══════════════════════════════════════════════════════════════
# BAGIAN 3: QUERY MENGGUNAKAN DATA RATINGS (user_rating field)
# ═══════════════════════════════════════════════════════════════

# 8. Top 10 film berdasarkan rata-rata user rating (min 100 vote)
q = list(films.find(
    {"user_rating.count": {"$gte": 100}},
    {"title": 1, "user_rating.avg": 1, "user_rating.count": 1, "_id": 0}
).sort("user_rating.avg", -1).limit(10))
show("Top 10 Film (User Rating, min 100 vote)", q)

# 9. Bandingkan TMDb rating vs User rating untuk satu film
q = films.find_one(
    {"title": "The Dark Knight"},
    {"title": 1, "tmdb_rating": 1, "user_rating.avg": 1,
     "user_rating.count": 1, "user_rating.std": 1, "_id": 0}
)
show("TMDb vs User Rating: The Dark Knight", q)

# 10. Film dengan user rating jauh LEBIH TINGGI dari TMDb rating
#     (hidden gems: disukai pengguna tapi kurang populer di TMDb)
q = list(films.aggregate([
    {"$match": {
        "user_rating": {"$ne": None},
        "tmdb_rating.count": {"$gte": 50},
        "user_rating.count": {"$gte": 50}
    }},
    {"$addFields": {
        "rating_gap": {"$subtract": ["$user_rating.avg", "$tmdb_rating.average"]}
    }},
    {"$sort": {"rating_gap": -1}},
    {"$limit": 5},
    {"$project": {
        "title": 1,
        "tmdb_avg": "$tmdb_rating.average",
        "user_avg": "$user_rating.avg",
        "rating_gap": {"$round": ["$rating_gap", 3]},
        "_id": 0
    }}
]))
show("Hidden Gems: User Rating >> TMDb Rating", q)

# 11. Distribusi histogram rating untuk satu film
q = films.find_one(
    {"title": "Forrest Gump"},
    {"title": 1, "user_rating.distribution": 1, "user_rating.avg": 1, "_id": 0}
)
show("Histogram Rating: Forrest Gump", q)

# ═══════════════════════════════════════════════════════════════
# BAGIAN 4: QUERY KOLEKSI user_ratings (raw ratings per film)
# ═══════════════════════════════════════════════════════════════

# 12. Ambil semua rating mentah untuk Toy Story
toy_story = films.find_one({"title": "Toy Story"}, {"_id": 1})
if toy_story:
    tmdb_id = toy_story["_id"]
    ur_doc  = ur_col.find_one({"_id": tmdb_id})
    if ur_doc:
        sample = {
            "tmdb_id"        : ur_doc["_id"],
            "total_ratings"  : len(ur_doc["ratings"]),
            "contoh_3_rating": ur_doc["ratings"][:3]
        }
        show("Raw Ratings: Toy Story (3 contoh pertama)", sample)

# 13. User yang paling banyak memberi rating (dari user_ratings collection)
q = list(ur_col.aggregate([
    {"$unwind": "$ratings"},
    {"$group": {
        "_id": "$ratings.userId",
        "total_rated": {"$sum": 1},
        "avg_rating_given": {"$avg": "$ratings.rating"}
    }},
    {"$sort": {"total_rated": -1}},
    {"$limit": 5},
    {"$project": {
        "userId": "$_id",
        "total_rated": 1,
        "avg_rating_given": {"$round": ["$avg_rating_given", 2]},
        "_id": 0
    }}
]))
show("Top 5 User Paling Aktif Memberi Rating", q)

# ═══════════════════════════════════════════════════════════════
# BAGIAN 5: AGREGASI LANJUTAN (gabungan semua data)
# ═══════════════════════════════════════════════════════════════

# 14. Genre dengan rata-rata USER rating tertinggi (bukan TMDb)
q = list(films.aggregate([
    {"$match": {"user_rating": {"$ne": None}, "user_rating.count": {"$gte": 30}}},
    {"$unwind": "$genres"},
    {"$group": {
        "_id": "$genres.name",
        "avg_user_rating": {"$avg": "$user_rating.avg"},
        "total_films": {"$sum": 1}
    }},
    {"$match": {"total_films": {"$gte": 20}}},
    {"$sort": {"avg_user_rating": -1}},
    {"$limit": 8},
    {"$project": {
        "genre": "$_id",
        "avg_user_rating": {"$round": ["$avg_user_rating", 3]},
        "total_films": 1, "_id": 0
    }}
]))
show("Genre Terbaik Versi Pengguna MovieLens", q)

# 15. Film paling menguntungkan dengan user rating tinggi
q = list(films.aggregate([
    {"$match": {
        "budget": {"$gt": 1_000_000},
        "revenue": {"$gt": 1_000_000},
        "user_rating.avg": {"$gte": 3.5}
    }},
    {"$addFields": {"profit": {"$subtract": ["$revenue", "$budget"]}}},
    {"$sort": {"profit": -1}},
    {"$limit": 5},
    {"$project": {
        "title": 1,
        "budget": 1,
        "revenue": 1,
        "profit": 1,
        "user_rating_avg": "$user_rating.avg",
        "tmdb_rating_avg": "$tmdb_rating.average",
        "_id": 0
    }}
]))
show("Film Paling Untung + User Rating ≥ 3.5", q)

# 16. Statistik keseluruhan database
total_films = films.count_documents({})
with_ratings = films.count_documents({"user_rating": {"$ne": None}})
with_links   = films.count_documents({"external_ids": {"$ne": None}})
ur_total     = ur_col.count_documents({})

stats_agg = list(films.aggregate([
    {"$group": {
        "_id": None,
        "avg_tmdb": {"$avg": "$tmdb_rating.average"},
        "avg_user": {"$avg": "$user_rating.avg"},
    }}
]))
s = stats_agg[0] if stats_agg else {}

show("Ringkasan Statistik Database", {
    "total_film"              : total_films,
    "film_dengan_user_rating" : with_ratings,
    "film_dengan_links"       : with_links,
    "koleksi_user_ratings"    : ur_total,
    "rata_tmdb_rating"        : round(s.get("avg_tmdb", 0), 3),
    "rata_user_rating"        : round(s.get("avg_user", 0) or 0, 3),
})

client.close()
print(f"\n✅ Semua query selesai dijalankan!")

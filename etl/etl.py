import pandas as pd
import json
import ast
import os
import sys
from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
from pymongo.errors import BulkWriteError
import time

# ─────────────────────────────────────────────────────────────────────────────
# KONFIGURASI
# ─────────────────────────────────────────────────────────────────────────────
DATA_DIR   = "/data"
MONGO_URI  = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
DB_NAME    = "movies_db"
COLL_FILMS = "films"
COLL_USERS = "user_ratings"   # koleksi terpisah untuk raw ratings per film
BATCH_SIZE = 500

# ─────────────────────────────────────────────────────────────────────────────
# HELPER
# ─────────────────────────────────────────────────────────────────────────────
def safe_parse(val):
    try:
        return ast.literal_eval(str(val))
    except (ValueError, SyntaxError):
        return []

def safe_float(val, default=0.0):
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def safe_int(val, default=0):
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default

# ─────────────────────────────────────────────────────────────────────────────
# STEP 1: BACA SEMUA CSV
# ─────────────────────────────────────────────────────────────────────────────
def read_csvs():
    print("\n[1/6] Membaca file CSV...")

    required = ["movies_metadata.csv", "credits.csv", "keywords.csv",
                "links.csv", "ratings.csv"]
    for f in required:
        path = os.path.join(DATA_DIR, f)
        if not os.path.exists(path):
            print(f"  ❌ File tidak ditemukan: {path}")
            print(f"     Letakkan file CSV dari Kaggle ke folder 'data/'")
            sys.exit(1)

    movies   = pd.read_csv(os.path.join(DATA_DIR, "movies_metadata.csv"), low_memory=False)
    credits  = pd.read_csv(os.path.join(DATA_DIR, "credits.csv"))
    keywords = pd.read_csv(os.path.join(DATA_DIR, "keywords.csv"))
    links    = pd.read_csv(os.path.join(DATA_DIR, "links.csv"))

    # ratings.csv bisa sangat besar (~26 juta baris), pakai dtype hemat memori
    print("  ⏳ Membaca ratings.csv (file besar, harap tunggu)...")
    ratings = pd.read_csv(
        os.path.join(DATA_DIR, "ratings.csv"),
        dtype={"userId": "int32", "movieId": "int32", "rating": "float32"},
        usecols=["userId", "movieId", "rating", "timestamp"]
    )

    print(f"  ✔ movies_metadata : {len(movies):,} baris")
    print(f"  ✔ credits         : {len(credits):,} baris")
    print(f"  ✔ keywords        : {len(keywords):,} baris")
    print(f"  ✔ links           : {len(links):,} baris")
    print(f"  ✔ ratings         : {len(ratings):,} baris")
    return movies, credits, keywords, links, ratings

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: BERSIHKAN & PARSE
# ─────────────────────────────────────────────────────────────────────────────
def clean_and_parse(movies, credits, keywords):
    print("\n[2/6] Membersihkan dan mem-parse data...")

    movies['id']   = movies['id'].astype(str).str.strip()
    credits['id']  = credits['id'].astype(str).str.strip()
    keywords['id'] = keywords['id'].astype(str).str.strip()

    # Buang ID non-numerik (ada beberapa baris rusak di dataset ini)
    movies = movies[movies['id'].str.match(r'^\d+$')].copy()

    list_cols = ['genres', 'production_companies', 'production_countries', 'spoken_languages']
    for col in list_cols:
        movies[col] = movies[col].apply(safe_parse)

    credits['cast'] = credits['cast'].apply(safe_parse)
    credits['crew'] = credits['crew'].apply(safe_parse)
    keywords['keywords'] = keywords['keywords'].apply(safe_parse)

    def get_directors(crew_list):
        if not isinstance(crew_list, list):
            return []
        return [{"name": p.get("name"), "id": p.get("id")}
                for p in crew_list if p.get("job") == "Director"]

    credits['directors'] = credits['crew'].apply(get_directors)
    credits['cast']      = credits['cast'].apply(
        lambda c: c[:10] if isinstance(c, list) else []
    )

    print("  ✔ Parsing selesai")
    return movies, credits, keywords

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: PROSES LINKS — buat lookup tmdbId → external IDs
# ─────────────────────────────────────────────────────────────────────────────
def process_links(links):
    """
    links.csv kolom: movieId (MovieLens), imdbId, tmdbId
    Output:
      - link_lookup : { tmdbId_str → { imdb_id, tmdb_id, movielens_id } }
      - links_clean : DataFrame bersih untuk dipakai proses ratings
    """
    print("\n[3/6] Memproses links.csv...")

    links_clean = links.dropna(subset=['tmdbId']).copy()
    links_clean['tmdbId']  = links_clean['tmdbId'].astype(int).astype(str)
    links_clean['movieId'] = links_clean['movieId'].astype(int)

    def fmt_imdb(val):
        try:
            return f"tt{int(val):07d}"
        except (ValueError, TypeError):
            return None

    links_clean['imdbId_fmt'] = links_clean['imdbId'].apply(fmt_imdb)

    link_lookup = {}
    for _, row in links_clean.iterrows():
        link_lookup[row['tmdbId']] = {
            "imdb_id"      : row['imdbId_fmt'],
            "tmdb_id"      : row['tmdbId'],
            "movielens_id" : int(row['movieId'])
        }

    print(f"  ✔ {len(link_lookup):,} entri link siap")
    return link_lookup, links_clean

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: PROSES RATINGS — agregasi + dokumen user_ratings
# ─────────────────────────────────────────────────────────────────────────────
def process_ratings(ratings, links_clean):
    """
    Strategi denormalisasi ratings:

    A) Statistik agregasi → di-embed langsung ke dokumen film
       Field: user_rating { avg, count, std, min, max, rating_distribution }

    B) Raw ratings per film → koleksi TERPISAH 'user_ratings'
       Dokumen: { _id: tmdbId, ratings: [{userId, rating, timestamp}, ...] }
       Alasan: data mentah bisa jutaan baris, tidak efisien di-embed ke film.
               Koleksi terpisah tetap bisa di-query dengan tmdbId yang sama.
    """
    print("\n[4/6] Memproses ratings.csv (ini langkah terlama)...")

    # Buat mapping: movieId (MovieLens int) → tmdbId (str)
    ml_to_tmdb = dict(
        zip(links_clean['movieId'].astype(int),
            links_clean['tmdbId'].astype(str))
    )

    # ── 4a. Agregasi statistik ──
    print("  ⏳ Menghitung statistik agregasi per film...")
    agg = ratings.groupby('movieId').agg(
        avg   = ('rating', 'mean'),
        count = ('rating', 'count'),
        std   = ('rating', 'std'),
        min_r = ('rating', 'min'),
        max_r = ('rating', 'max'),
    ).reset_index()
    agg['avg'] = agg['avg'].round(4)
    agg['std'] = agg['std'].round(4).fillna(0.0)

    # Distribusi: hitung berapa rating per nilai bintang (0.5 s/d 5.0)
    print("  ⏳ Menghitung distribusi histogram rating...")
    star_values = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]

    def build_distribution(group):
        dist = {}
        for s in star_values:
            dist[str(s)] = int((group == s).sum())
        return dist

    dist_series = ratings.groupby('movieId')['rating'].apply(build_distribution)
    dist_df     = dist_series.reset_index()
    dist_df.columns = ['movieId', 'distribution']
    agg = agg.merge(dist_df, on='movieId', how='left')

    # Map ke tmdbId
    agg['tmdb_id'] = agg['movieId'].map(ml_to_tmdb)
    agg = agg.dropna(subset=['tmdb_id'])

    rating_lookup = {}
    for _, row in agg.iterrows():
        rating_lookup[str(row['tmdb_id'])] = {
            "avg"          : float(row['avg']),
            "count"        : int(row['count']),
            "std"          : float(row['std']),
            "min"          : float(row['min_r']),
            "max"          : float(row['max_r']),
            "distribution" : row['distribution'],
        }

    print(f"  ✔ Agregasi siap untuk {len(rating_lookup):,} film")

    # ── 4b. Raw ratings per film untuk koleksi user_ratings ──
    print("  ⏳ Menyiapkan dokumen user_ratings (per film)...")
    ratings['tmdb_id'] = ratings['movieId'].map(ml_to_tmdb)
    valid = ratings.dropna(subset=['tmdb_id']).copy()
    valid['tmdb_id'] = valid['tmdb_id'].astype(str)

    user_rating_docs = []
    grouped = valid.groupby('tmdb_id')
    for tmdb_id, group in grouped:
        entries = [
            {
                "userId"    : int(r['userId']),
                "rating"    : float(r['rating']),
                "timestamp" : int(r['timestamp'])
            }
            for _, r in group.iterrows()
        ]
        user_rating_docs.append({
            "_id"    : tmdb_id,
            "ratings": entries
        })

    total_entries = valid.shape[0]
    print(f"  ✔ {len(user_rating_docs):,} dokumen user_ratings ({total_entries:,} total entri)")
    return rating_lookup, user_rating_docs

# ─────────────────────────────────────────────────────────────────────────────
# STEP 5: MERGE & DENORMALISASI SEMUA DATA → DOKUMEN FILM FINAL
# ─────────────────────────────────────────────────────────────────────────────
def merge_and_denormalize(movies, credits, keywords, link_lookup, rating_lookup):
    print("\n[5/6] Menggabungkan semua dataset (denormalisasi)...")

    merged = movies.merge(credits[['id','cast','directors']], on='id', how='left')
    merged = merged.merge(keywords[['id','keywords']],        on='id', how='left')

    for col in ['cast', 'directors', 'keywords', 'genres',
                'production_companies', 'production_countries', 'spoken_languages']:
        merged[col] = merged[col].apply(lambda x: x if isinstance(x, list) else [])

    docs = []
    for _, row in merged.iterrows():
        tmdb_id = row['id']
        doc = {
            "_id"               : tmdb_id,
            "title"             : str(row.get('title', '')),
            "original_title"    : str(row.get('original_title', '')),
            "release_date"      : str(row.get('release_date', '')),
            "budget"            : safe_int(row.get('budget', 0)),
            "revenue"           : safe_int(row.get('revenue', 0)),
            "runtime"           : safe_float(row.get('runtime', 0)),
            "status"            : str(row.get('status', '')),
            "tagline"           : str(row.get('tagline', '')),
            "overview"          : str(row.get('overview', '')),
            "popularity"        : safe_float(row.get('popularity', 0)),
            "original_language" : str(row.get('original_language', '')),

            # ── Rating dari TMDb (votes di metadata asli) ──
            "tmdb_rating": {
                "average" : safe_float(row.get('vote_average', 0)),
                "count"   : safe_int(row.get('vote_count', 0)),
            },

            # ── Rating dari pengguna MovieLens (agregasi ratings.csv) ──
            # Jika film tidak ada di ratings.csv, field ini None
            "user_rating": rating_lookup.get(tmdb_id),

            # ── External IDs dari links.csv ──
            # { imdb_id: "tt0114709", tmdb_id: "862", movielens_id: 1 }
            "external_ids": link_lookup.get(tmdb_id),

            # ── Nested relational data (denormalized) ──
            "genres"               : row['genres'],
            "keywords"             : row['keywords'],
            "cast"                 : row['cast'],
            "directors"            : row['directors'],
            "production_companies" : row['production_companies'],
            "production_countries" : row['production_countries'],
            "spoken_languages"     : row['spoken_languages'],
        }
        docs.append(doc)

    # Deduplikat berdasarkan _id
    seen, unique_docs = set(), []
    for d in docs:
        if d['_id'] not in seen and d['title']:
            seen.add(d['_id'])
            unique_docs.append(d)

    rated_count = sum(1 for d in unique_docs if d['user_rating'] is not None)
    linked_count = sum(1 for d in unique_docs if d['external_ids'] is not None)
    print(f"  ✔ Total dokumen film        : {len(unique_docs):,}")
    print(f"  ✔ Film dengan user_rating   : {rated_count:,}")
    print(f"  ✔ Film dengan external_ids  : {linked_count:,}")
    return unique_docs

# ─────────────────────────────────────────────────────────────────────────────
# STEP 6: IMPORT KE MONGODB
# ─────────────────────────────────────────────────────────────────────────────
def import_to_mongo(docs, user_rating_docs):
    print("\n[6/6] Mengimpor ke MongoDB...")

    print("  ⏳ Menunggu MongoDB siap...")
    for attempt in range(30):
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
            client.server_info()
            break
        except Exception:
            print(f"  ... percobaan {attempt+1}/30")
            time.sleep(2)
    else:
        print("  ❌ MongoDB tidak bisa dijangkau setelah 60 detik!")
        sys.exit(1)

    db = client[DB_NAME]

    def bulk_insert(collection, data, label):
        collection.drop()
        total = 0
        for i in range(0, len(data), BATCH_SIZE):
            batch = data[i:i+BATCH_SIZE]
            try:
                collection.insert_many(batch, ordered=False)
                total += len(batch)
            except BulkWriteError as bwe:
                total += bwe.details.get('nInserted', 0)
            pct = (min(total, len(data)) / len(data)) * 100
            print(f"  [{label}] {total:,}/{len(data):,} ({pct:.1f}%)", end='\r')
        print(f"\n  ✔ [{label}] {total:,} dokumen diimpor")

    # ── Koleksi: films ──
    col_films = db[COLL_FILMS]
    bulk_insert(col_films, docs, "films")

    print("  Membuat index untuk koleksi 'films'...")
    col_films.create_index([("title", TEXT), ("overview", TEXT)],        name="text_search")
    col_films.create_index([("genres.name", ASCENDING)],                  name="idx_genre")
    col_films.create_index([("tmdb_rating.average", DESCENDING)],         name="idx_tmdb_rating")
    col_films.create_index([("user_rating.avg", DESCENDING)],             name="idx_user_rating_avg")
    col_films.create_index([("user_rating.count", DESCENDING)],           name="idx_user_rating_count")
    col_films.create_index([("release_date", ASCENDING)],                 name="idx_release")
    col_films.create_index([("directors.name", ASCENDING)],               name="idx_director")
    col_films.create_index([("cast.name", ASCENDING)],                    name="idx_cast")
    col_films.create_index([("external_ids.imdb_id", ASCENDING)],         name="idx_imdb")
    col_films.create_index([("external_ids.movielens_id", ASCENDING)],    name="idx_movielens")
    print("  ✔ 10 index dibuat untuk koleksi 'films'")

    # ── Koleksi: user_ratings ──
    if user_rating_docs:
        col_ur = db[COLL_USERS]
        bulk_insert(col_ur, user_rating_docs, "user_ratings")
        col_ur.create_index([("_id", ASCENDING)], name="idx_tmdb_id")
        print("  ✔ Index dibuat untuk koleksi 'user_ratings'")

    # ── Ringkasan ──
    films_count = col_films.count_documents({})
    ur_count    = db[COLL_USERS].count_documents({}) if user_rating_docs else 0
    print(f"\n  ✔ Koleksi 'films'        : {films_count:,} dokumen")
    print(f"  ✔ Koleksi 'user_ratings' : {ur_count:,} dokumen")
    client.close()

# ─────────────────────────────────────────────────────────────────────────────
# OPSIONAL: SIMPAN BACKUP JSON
# ─────────────────────────────────────────────────────────────────────────────
def save_json(docs):
    print("\n  Menyimpan backup JSON...")
    out = os.path.join(DATA_DIR, "movies_nosql.json")
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(docs, f, ensure_ascii=False, default=str)
    mb = os.path.getsize(out) / 1024 / 1024
    print(f"  ✔ Backup tersimpan: {out} ({mb:.1f} MB)")

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 65)
    print("  ETL: The Movies Dataset (5 CSV) → MongoDB Document Store")
    print("=" * 65)

    movies, credits, keywords, links, ratings = read_csvs()
    movies, credits, keywords                 = clean_and_parse(movies, credits, keywords)
    link_lookup, links_clean                  = process_links(links)
    rating_lookup, user_rating_docs           = process_ratings(ratings, links_clean)
    docs                                      = merge_and_denormalize(
                                                    movies, credits, keywords,
                                                    link_lookup, rating_lookup)
    save_json(docs)
    import_to_mongo(docs, user_rating_docs)

    print("\n" + "=" * 65)
    print("  ✅ ETL SELESAI! Semua 5 dataset telah diproses.")
    print()
    print("  Koleksi MongoDB yang tersedia:")
    print("    • films        → dokumen lengkap per film (semua data embed)")
    print("    • user_ratings → raw ratings per film dari MovieLens")
    print()
    print("  Akses:")
    print("    🌐 Mongo Express : http://localhost:8081  (admin / admin123)")
    print("    🔗 Compass URI   : mongodb://localhost:27017")
    print("=" * 65)

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
# STEP 1: BACA CSV
# ─────────────────────────────────────────────────────────────────────────────
def read_csvs():
    print("\n[1/4] Membaca file CSV...")

    required = ["movies_metadata.csv", "credits.csv", "keywords.csv", "links.csv"]
    for f in required:
        path = os.path.join(DATA_DIR, f)
        if not os.path.exists(path):
            print(f"  ❌ File tidak ditemukan: {path}")
            sys.exit(1)

    movies   = pd.read_csv(os.path.join(DATA_DIR, "movies_metadata.csv"), low_memory=False)
    credits  = pd.read_csv(os.path.join(DATA_DIR, "credits.csv"))
    keywords = pd.read_csv(os.path.join(DATA_DIR, "keywords.csv"))
    links    = pd.read_csv(os.path.join(DATA_DIR, "links.csv"))

    print(f"  ✔ movies_metadata : {len(movies):,} baris")
    print(f"  ✔ credits         : {len(credits):,} baris")
    print(f"  ✔ keywords        : {len(keywords):,} baris")
    print(f"  ✔ links           : {len(links):,} baris")
    return movies, credits, keywords, links

# ─────────────────────────────────────────────────────────────────────────────
# STEP 2: BERSIHKAN & PARSE
# ─────────────────────────────────────────────────────────────────────────────
def clean_and_parse(movies, credits, keywords, links):
    print("\n[2/4] Membersihkan dan mem-parse data...")

    # Normalisasi ID
    movies['id']   = movies['id'].astype(str).str.strip()
    credits['id']  = credits['id'].astype(str).str.strip()
    keywords['id'] = keywords['id'].astype(str).str.strip()

    # Buang baris rusak (ID non-numerik)
    movies = movies[movies['id'].str.match(r'^\d+$')].copy()
    movies = movies.drop_duplicates(subset=['id'])

    # Parse kolom JSON-string di movies
    for col in ['genres', 'production_companies', 'production_countries', 'spoken_languages']:
        movies[col] = movies[col].apply(safe_parse)

    # Parse credits
    credits['cast'] = credits['cast'].apply(safe_parse)
    credits['crew'] = credits['crew'].apply(safe_parse)
    credits = credits.drop_duplicates(subset=['id'])

    # Parse keywords
    keywords['keywords'] = keywords['keywords'].apply(safe_parse)
    keywords = keywords.drop_duplicates(subset=['id'])

    # Ekstrak sutradara dari crew
    def get_directors(crew_list):
        if not isinstance(crew_list, list):
            return []
        return [{"name": p.get("name"), "id": p.get("id")}
                for p in crew_list if p.get("job") == "Director"]

    credits['directors'] = credits['crew'].apply(get_directors)
    credits['cast']      = credits['cast'].apply(lambda c: c[:10] if isinstance(c, list) else [])

    # Proses links → external_ids lookup
    links_clean = links.dropna(subset=['tmdbId']).copy()
    links_clean['tmdbId']  = links_clean['tmdbId'].astype(int).astype(str)
    links_clean['movieId'] = links_clean['movieId'].astype(int)

    def fmt_imdb(val):
        try:
            return f"tt{int(val):07d}"
        except (ValueError, TypeError):
            return None

    links_clean['imdbId_fmt'] = links_clean['imdbId'].apply(fmt_imdb)

    link_lookup = {
        row['tmdbId']: {
            "imdb_id"      : row['imdbId_fmt'],
            "tmdb_id"      : row['tmdbId'],
            "movielens_id" : int(row['movieId'])
        }
        for _, row in links_clean.iterrows()
    }

    print(f"  ✔ Parsing selesai")
    print(f"  ✔ {len(link_lookup):,} external links siap")
    return movies, credits, keywords, link_lookup

# ─────────────────────────────────────────────────────────────────────────────
# STEP 3: MERGE & DENORMALISASI
# ─────────────────────────────────────────────────────────────────────────────
def merge_and_denormalize(movies, credits, keywords, link_lookup):
    print("\n[3/4] Menggabungkan semua dataset (denormalisasi)...")

    merged = movies.merge(credits[['id', 'cast', 'directors']], on='id', how='left')
    merged = merged.merge(keywords[['id', 'keywords']],         on='id', how='left')

    # Pastikan semua kolom list tidak NaN
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
            "tmdb_rating": {
                "average" : safe_float(row.get('vote_average', 0)),
                "count"   : safe_int(row.get('vote_count', 0)),
            },
            "external_ids"         : link_lookup.get(tmdb_id),
            "genres"               : row['genres'],
            "keywords"             : row['keywords'],
            "cast"                 : row['cast'],
            "directors"            : row['directors'],
            "production_companies" : row['production_companies'],
            "production_countries" : row['production_countries'],
            "spoken_languages"     : row['spoken_languages'],
        }
        docs.append(doc)

    # Deduplikat
    seen, unique_docs = set(), []
    for d in docs:
        if d['_id'] not in seen and d['title']:
            seen.add(d['_id'])
            unique_docs.append(d)

    linked = sum(1 for d in unique_docs if d['external_ids'] is not None)
    print(f"  ✔ Total dokumen       : {len(unique_docs):,}")
    print(f"  ✔ Dengan external_ids : {linked:,}")
    return unique_docs

# ─────────────────────────────────────────────────────────────────────────────
# STEP 4: IMPORT KE MONGODB
# ─────────────────────────────────────────────────────────────────────────────
def import_to_mongo(docs):
    print("\n[4/4] Mengimpor ke MongoDB...")

    print("  ⏳ Menunggu MongoDB siap...")
    for attempt in range(30):
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=2000)
            client.server_info()
            break
        except Exception:
            print(f"  ... percobaan {attempt + 1}/30")
            time.sleep(2)
    else:
        print("  ❌ MongoDB tidak bisa dijangkau!")
        sys.exit(1)

    db        = client[DB_NAME]
    col_films = db[COLL_FILMS]
    col_films.drop()

    total = 0
    for i in range(0, len(docs), BATCH_SIZE):
        batch = docs[i:i + BATCH_SIZE]
        try:
            col_films.insert_many(batch, ordered=False)
            total += len(batch)
        except BulkWriteError as bwe:
            total += bwe.details.get('nInserted', 0)
        pct = (min(total, len(docs)) / len(docs)) * 100
        print(f"  {total:,}/{len(docs):,} ({pct:.1f}%)", end='\r')

    print(f"\n  ✔ {total:,} dokumen diimpor")

    print("  Membuat index...")
    col_films.create_index([("title", TEXT), ("overview", TEXT)],     name="text_search")
    col_films.create_index([("genres.name", ASCENDING)],               name="idx_genre")
    col_films.create_index([("tmdb_rating.average", DESCENDING)],      name="idx_rating")
    col_films.create_index([("release_date", ASCENDING)],              name="idx_release")
    col_films.create_index([("directors.name", ASCENDING)],            name="idx_director")
    col_films.create_index([("cast.name", ASCENDING)],                 name="idx_cast")
    col_films.create_index([("external_ids.imdb_id", ASCENDING)],      name="idx_imdb")
    print("  ✔ 7 index dibuat")

    count = col_films.count_documents({})
    print(f"  ✔ Verifikasi: {count:,} dokumen tersimpan di MongoDB")
    client.close()

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  ETL: The Movies Dataset → MongoDB Document Store")
    print("  (movies + credits + keywords + links)")
    print("=" * 60)

    movies, credits, keywords, links = read_csvs()
    movies, credits, keywords, link_lookup = clean_and_parse(movies, credits, keywords, links)
    docs = merge_and_denormalize(movies, credits, keywords, link_lookup)
    import_to_mongo(docs)

    print("\n" + "=" * 60)
    print("  ✅ ETL SELESAI!")
    print("  🌐 Mongo Express : http://localhost:8081  (admin / admin123)")
    print("  🔗 Compass URI   : mongodb://localhost:27017")
    print("=" * 60)

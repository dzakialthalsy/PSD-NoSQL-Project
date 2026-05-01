import pandas as pd
import json
import ast
import os
import sys
from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
from pymongo.errors import BulkWriteError
import time

# --- KONFIGURASI ---
DATA_DIR   = "/data"
MONGO_URI  = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
DB_NAME    = "movies_db"
COLL_FILMS = "films"
COLL_USERS = "user_ratings"
BATCH_SIZE = 1000  # Dinaikkan untuk kecepatan

# --- HELPER (Tetap Sama) ---
def safe_parse(val):
    try: return ast.literal_eval(str(val))
    except: return []

def safe_float(val, default=0.0):
    try: return float(val)
    except: return default

def safe_int(val, default=0):
    try: return int(float(val))
    except: return default

# [1/6] BACA CSV
def read_csvs():
    print("\n[1/6] Membaca file CSV...")
    movies   = pd.read_csv(os.path.join(DATA_DIR, "movies_metadata.csv"), low_memory=False)
    links    = pd.read_csv(os.path.join(DATA_DIR, "links.csv"))
    credits  = pd.read_csv(os.path.join(DATA_DIR, "credits.csv"))
    keywords = pd.read_csv(os.path.join(DATA_DIR, "keywords.csv"))
    
    # Gunakan ratings_small.csv untuk pengembangan agar tidak crash, 
    # atau pastikan RAM cukup untuk ratings.csv
    rating_file = "ratings_small.csv" if os.path.exists(os.path.join(DATA_DIR, "ratings_small.csv")) else "ratings.csv"
    print(f"  ⏳ Membaca {rating_file}...")
    ratings = pd.read_csv(os.path.join(DATA_DIR, rating_file))
    
    return movies, credits, keywords, links, ratings

# [2/6] CLEAN & PARSE (Optimasi Cast & Director)
def clean_and_parse(movies, credits, keywords):
    print("\n[2/6] Membersihkan data...")
    movies = movies[movies['id'].str.match(r'^\d+$')].copy()
    movies['id'] = movies['id'].astype(int)
    
    for col in ['genres', 'production_companies']:
        movies[col] = movies[col].apply(safe_parse)

    credits['id'] = pd.to_numeric(credits['id'], errors='coerce')
    credits = credits.dropna(subset=['id'])
    credits['cast'] = credits['cast'].apply(lambda x: safe_parse(x)[:5]) # Ambil 5 aktor saja agar dokumen ringkas
    
    keywords['id'] = pd.to_numeric(keywords['id'], errors='coerce')
    keywords['keywords'] = keywords['keywords'].apply(safe_parse)
    
    return movies, credits, keywords

# [3/6] PROCESS LINKS
def process_links(links):
    print("\n[3/6] Memproses links...")
    links = links.dropna(subset=['tmdbId'])
    links['tmdbId'] = links['tmdbId'].astype(int)
    links['movieId'] = links['movieId'].astype(int)
    return dict(zip(links['movieId'], links['tmdbId']))

# [4/6] PROCESS RATINGS (Perbaikan Utama: Kecepatan)
def process_ratings(ratings, ml_to_tmdb):
    print("\n[4/6] Menghitung agregasi rating...")
    # Agregasi dasar yang cepat
    agg = ratings.groupby('movieId').agg(
        avg=('rating', 'mean'),
        count=('rating', 'count')
    ).reset_index()
    
    agg['tmdb_id'] = agg['movieId'].map(ml_to_tmdb)
    agg = agg.dropna(subset=['tmdb_id'])
    agg['tmdb_id'] = agg['tmdb_id'].astype(int)
    
    rating_lookup = {}
    for _, row in agg.iterrows():
        rating_lookup[row['tmdb_id']] = {
            "average": round(float(row['avg']), 2),
            "vote_count": int(row['count'])
        }
    return rating_lookup

# [5/6] MERGE & DENORMALIZE
def merge_and_denormalize(movies, credits, keywords, rating_lookup):
    print("\n[5/6] Menggabungkan data film...")
    merged = movies.merge(credits[['id', 'cast']], on='id', how='left')
    merged = merged.merge(keywords[['id', 'keywords']], on='id', how='left')
    
    docs = []
    for _, row in merged.iterrows():
        m_id = int(row['id'])
        # Gabungkan metadata film dengan rating yang sudah dihitung
        doc = {
            "_id": m_id,
            "title": row.get('title'),
            "genres": [g['name'] for g in row['genres']] if isinstance(row['genres'], list) else [],
            "cast": [c['name'] for c in row['cast']] if isinstance(row['cast'], list) else [],
            "runtime": safe_int(row.get('runtime')),
            "release_date": row.get('release_date'),
            # Kolom ini yang Anda cari:
            "ratings": rating_lookup.get(m_id, {"average": 0, "vote_count": 0})
        }
        docs.append(doc)
    return docs

# [6/6] IMPORT
def import_to_mongo(docs):
    print("\n[6/6] Mengirim ke MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col = db[COLL_FILMS]
    col.drop() # Bersihkan data lama
    
    if docs:
        col.insert_many(docs)
        print(f"  ✅ Berhasil mengimpor {len(docs)} film ke koleksi 'films'")
    client.close()

if __name__ == "__main__":
    m, c, k, l, r = read_csvs()
    m, c, k = clean_and_parse(m, c, k)
    ml_to_tmdb = process_links(l)
    rating_lookup = process_ratings(r, ml_to_tmdb)
    final_docs = merge_and_denormalize(m, c, k, rating_lookup)
    import_to_mongo(final_docs)
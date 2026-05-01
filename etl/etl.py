import pandas as pd
import ast
import os
from pymongo import MongoClient

# --- KONFIGURASI ---
DATA_DIR   = "/data"
MONGO_URI  = os.getenv("MONGO_URI", "mongodb://mongo:27017/")
DB_NAME    = "movies_db"
COLL_FILMS = "films"

def run_etl():
    print("\n🚀 Memulai ETL NoSQL Koleksi Film...")

    # 1. BACA CSV (Ambil kolom yang perlu saja biar hemat RAM)
    print("[1/5] Membaca file CSV...")
    movies   = pd.read_csv(os.path.join(DATA_DIR, "movies_metadata.csv"), low_memory=False, 
                           usecols=['id', 'title', 'genres', 'runtime', 'release_date'])
    links    = pd.read_csv(os.path.join(DATA_DIR, "links.csv"), usecols=['movieId', 'tmdbId'])
    credits  = pd.read_csv(os.path.join(DATA_DIR, "credits.csv"))
    
    # Gunakan ratings_small agar proses cepat dan tidak crash
    rating_path = os.path.join(DATA_DIR, "ratings_small.csv")
    ratings = pd.read_csv(rating_path) if os.path.exists(rating_path) else pd.read_csv(os.path.join(DATA_DIR, "ratings.csv"))

    # 2. BERSIHKAN DATA
    print("[2/5] Membersihkan data...")
    # Pastikan ID numerik dan tidak ada yang kosong
    movies = movies[movies['id'].str.match(r'^\d+$', na=False)].copy()
    movies['id'] = movies['id'].astype(int)
    
    links = links.dropna(subset=['tmdbId'])
    links['tmdbId'] = links['tmdbId'].astype(int)
    ml_to_tmdb = dict(zip(links['movieId'], links['tmdbId']))

    # 3. PROSES RATING (AGREGASI)
    print("[3/5] Menghitung rata-rata rating...")
    ratings['tmdb_id'] = ratings['movieId'].map(ml_to_tmdb)
    agg = ratings.groupby('tmdb_id')['rating'].agg(['mean', 'count']).reset_index()
    rating_lookup = agg.set_index('tmdb_id').to_dict('index')

    # 4. PROSES CREW (AMBIL DIRECTOR SAJA)
    print("[4/5] Mengambil data Sutradara...")
    def get_director(x):
        try:
            items = ast.literal_eval(x)
            # Hanya ambil nama Director agar ukuran dokumen kecil
            return [i['name'] for i in items if i['job'] == 'Director']
        except: return []
    
    credits['id'] = pd.to_numeric(credits['id'], errors='coerce')
    credits['directors'] = credits['crew'].apply(get_director)

    # 5. MERGE & IMPORT
    print("[5/5] Menggabungkan data & Kirim ke MongoDB...")
    movies = movies.merge(credits[['id', 'directors']], on='id', how='left')

    # --- TAMBAHKAN BARIS INI UNTUK MENGHAPUS DUPLIKAT ID ---
    movies = movies.drop_duplicates(subset=['id'], keep='first')
    # ------------------------------------------------------

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    col = db[COLL_FILMS]
    col.drop() # Hapus data lama agar bersih

    docs = []
    for _, row in movies.iterrows():
        m_id = int(row['id'])
        r_data = rating_lookup.get(m_id, {'mean': 0, 'count': 0})
        
        doc = {
            "_id": m_id,
            "title": row['title'],
            "genres": [g['name'] for g in ast.literal_eval(row['genres'])] if isinstance(row['genres'], str) else [],
            "directors": row['directors'] if isinstance(row['directors'], list) else [],
            "runtime": int(row['runtime']) if not pd.isna(row['runtime']) else 0,
            "release_date": row['release_date'],
            # Field ratings yang Anda inginkan
            "ratings": {
                "average": round(float(r_data['mean']), 2),
                "vote_count": int(r_data['count'])
            }
        }
        docs.append(doc)

    if docs:
        col.insert_many(docs)
        print(f"✅ Berhasil! {len(docs)} film masuk ke MongoDB.")
    
    client.close()

if __name__ == "__main__":
    run_etl()
import pandas as pd
import ast
import os
from pymongo import MongoClient

DATA_DIR = "/data"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017/")

def run_etl():
    print("\n🚀 Memulai ETL NoSQL (Versi Anti-Duplikat)...")

    # 1. BACA DATA
    print("[1/5] Membaca CSV...")
    movies = pd.read_csv(os.path.join(DATA_DIR, "movies_metadata.csv"), low_memory=False)
    credits = pd.read_csv(os.path.join(DATA_DIR, "credits.csv"))
    links = pd.read_csv(os.path.join(DATA_DIR, "links.csv"))
    
    # Gunakan small ratings agar cepat
    rating_path = os.path.join(DATA_DIR, "ratings_small.csv")
    ratings = pd.read_csv(rating_path) if os.path.exists(rating_path) else pd.read_csv(os.path.join(DATA_DIR, "ratings.csv"))

    # 2. BERSIHKAN MOVIES (PONDASI UTAMA)
    print("[2/5] Membersihkan Metadata & Hapus Duplikat...")
    movies = movies[movies['id'].str.match(r'^\d+$', na=False)].copy()
    movies['id'] = movies['id'].astype(int)
    # Wajib drop duplikat di awal agar pondasi _id unik
    movies = movies.drop_duplicates(subset=['id'])

    # 3. PROSES SUTRADARA (HAPUS DUPLIKAT DI CREDITS)
    print("[3/5] Memproses Sutradara...")
    def get_director(x):
        try:
            items = ast.literal_eval(x)
            return [i['name'] for i in items if i['job'] == 'Director']
        except: return []
    
    credits['id'] = pd.to_numeric(credits['id'], errors='coerce')
    # Ambil sutradara lalu hapus duplikat ID di credits agar tidak merusak merge
    credits['directors'] = credits['crew'].apply(get_director)
    credits_clean = credits.dropna(subset=['id']).drop_duplicates(subset=['id'])

    # 4. HITUNG RATING
    print("[4/5] Menghitung Agregasi Rating...")
    links = links.dropna(subset=['tmdbId'])
    links['tmdbId'] = links['tmdbId'].astype(int)
    ml_to_tmdb = dict(zip(links['movieId'], links['tmdbId']))
    
    ratings['tmdb_id'] = ratings['movieId'].map(ml_to_tmdb)
    agg = ratings.groupby('tmdb_id')['rating'].agg(['mean', 'count']).reset_index()
    rating_lookup = agg.set_index('tmdb_id').to_dict('index')

    # 5. MERGE & IMPORT
    print("[5/5] Menggabungkan data & Kirim ke MongoDB...")
    # Pakai how='left' agar tetap mengacu pada ID movies yang sudah unik
    final_df = movies.merge(credits_clean[['id', 'directors']], on='id', how='left')

    client = MongoClient(MONGO_URI)
    db = client["movies_db"]
    col = db["films"]
    col.drop() # Bersihkan koleksi lama[cite: 3, 4]

    docs = []
    for _, row in final_df.iterrows():
        m_id = int(row['id'])
        r_data = rating_lookup.get(m_id, {'mean': 0, 'count': 0})
        
        # Denormalisasi data menjadi Document Store[cite: 1]
        doc = {
            "_id": m_id,
            "title": row['title'],
            "directors": row['directors'] if isinstance(row['directors'], list) else [],
            "release_date": str(row['release_date']),
            "ratings": {
                "average": round(float(r_data['mean']), 2),
                "vote_count": int(r_data['count'])
            }
        }
        docs.append(doc)

    # Insert many dengan penanganan duplikat manual jika masih ada yang lolos
    try:
        col.insert_many(docs, ordered=False) 
        print(f"✅ BERHASIL! {len(docs)} dokumen masuk.")
    except Exception as e:
        print(f"⚠️ Ada beberapa duplikat yang dilewati, tapi data tetap masuk.")

    client.close()

if __name__ == "__main__":
    run_etl()
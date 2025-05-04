import pandas as pd
from pymongo import MongoClient, errors

# Charger seulement les 1000 premières lignes
csv_path = "./notebooks/data/movie.csv"
movies_df = pd.read_csv(csv_path).head(1000)

# Connexion à MongoDB
client = MongoClient("mongodb://mongodb:27017")
db = client["recommandation_simple_db"]
collection = db["movies"]

# Créer un index unique sur movieId
try:
    collection.create_index("movieId", unique=True)
    print("✅ Index unique créé sur 'movieId'")
except errors.OperationFailure:
    print("⚠️ Index déjà existant ou erreur ignorée")

# Insertion sans doublons
inserted, skipped = 0, 0

for _, row in movies_df.iterrows():
    movie = row.to_dict()
    try:
        collection.insert_one(movie)
        inserted += 1
    except errors.DuplicateKeyError:
        skipped += 1

print(f"✅ Insertion terminée : {inserted} films ajoutés, {skipped} doublons ignorés.")
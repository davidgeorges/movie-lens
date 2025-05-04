# kafka_consumer.py

from kafka import KafkaConsumer
import json
from pymongo import MongoClient, errors

# ================================
# Configuration Kafka & MongoDB
# ================================
TOPIC_NAME = 'movie_rating'
KAFKA_SERVER = 'bigdata:9092'
MONGO_URI = "mongodb://mongodb:27017"
DB_NAME = "recommandation_simple_db"
COLLECTION_NAME = "ratings_streamed"

# ================================
# Connexion MongoDB
# ================================
try:
    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[DB_NAME]
    collection = mongo_db[COLLECTION_NAME]
    print(f"✅ Connecté à MongoDB : {DB_NAME}.{COLLECTION_NAME}")
except errors.ConnectionFailure as e:
    print(f"❌ Erreur de connexion MongoDB : {e}")
    exit(1)

# ================================
# Initialisation du consommateur Kafka
# ================================
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='movie_rating_consumer_group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# ================================
# Consommation + Insertion Mongo
# ================================
print(f"🎧 En attente de messages sur le topic : '{TOPIC_NAME}'...")

try:
    for message in consumer:
        rating = message.value
        print(f"🎬 Nouveau rating reçu : {rating}")

        try:
            collection.insert_one(rating)
            print("✅ Insertion MongoDB réussie")
        except Exception as e:
            print(f"❌ Échec insertion MongoDB : {e}")

except KeyboardInterrupt:
    print("🛑 Arrêt manuel du consommateur Kafka.")
finally:
    consumer.close()
    mongo_client.close()
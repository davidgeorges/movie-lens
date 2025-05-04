# kafka_producer.py

#Importation des bibliothèques
from kafka import KafkaProducer
import json
import time
import random

# ================================
# Configuration Kafka
# ================================
TOPIC_NAME = 'movie_rating'
KAFKA_SERVER = 'bigdata:9092'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ================================
# Simulation d'une note utilisateur
# ================================
def generate_rating():
    return {
        'userId': random.randint(0, 137999),     # 138000 users
        'movieId': random.randint(0, 999),      # 26744 movies
        'rating': round(random.uniform(0.5, 5.0), 1),
        'timestamp': int(time.time())
    }


# ================================
# Envoi des ratings dans Kafka
# ================================
def send_ratings():
    print(f"🔄 Envoi des ratings vers le topic Kafka : '{TOPIC_NAME}'")
    try:
        while True:
            rating = generate_rating()
            producer.send(TOPIC_NAME, value=rating)
            print(f"✅ Rating envoyé : {rating}")
            time.sleep(2)
    except Exception as e:
        print(f"❌ Erreur lors de l'envoi : {e}")
    finally:
        producer.close()
        print("🚫 Producteur Kafka fermé.")


# ================================
# Lancement
# ================================
if __name__ == "__main__":
    send_ratings()
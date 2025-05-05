# MOVIE LENS

## 🌍 Project Overview
Ce projet fournit un environnement Dockerisé prêt à l'emploi pour travailler avec :
- **Hadoop 3.3.6** (pseudo-distribué)
- **Spark 3.5.1** (mode autonome)
- **Kafka 3.7.2** (avec Zookeeper)
- **Python 3** + **PySpark**
- **Jupyter Notebook**

## 📊 Architecture
- Hadoop HDFS pour le stockage distribué des fichiers (configuration mono-nœud)
- Spark pour le traitement des données par lots et en streaming
- Kafka pour l'ingestion en streaming
- Environnement Python avec Jupyter pour le développement et l'expérimentation

## ! IMPORTANT !
Veuillez mettre movie.csv et rating.csv dans le dossier notebooks/data/
Exemple : notebooks/data/movies.csv

## 🔄 Quick Start

### 1. Build the Docker Image
```bash
docker-compose down
docker-compose up -d --build
docker logs -f bigdata-container
```

## 📄 Notebooks & Scripts
- **kafka_producer.py** : Génère des données aléatoires et les envoie vers un topic Kafka.
Exemple de logs :
```bash
✅ Rating envoyé : {'userId': 130130, 'movieId': 883, 'rating': 2.7, 'timestamp': 1746443786}
```
- **kafka_consumer.py** : Consomme des données depuis un topic Kafka et les insére en base de données.
Exemple de logs :
```bash
🎬 Nouveau rating reçu : {'userId': 130130, 'movieId': 137, 'rating': 2.4, 'timestamp': 1746397727}
✅ Insertion MongoDB réussie
```
- **insert_movies_unique.py** : Insére les données de movies.csv dans la base de données.
Exemple de logs :
```bash
✅ Insertion terminée : 1000 films ajoutés, 0 doublons ignorés.
```

## 🔔 Notes
- Notebook access: [http://localhost:8888](http://localhost:8888)
- Streamklit UI : [http://localhost:8501](http://localhost:8501)
- Hadoop HDFS Web UI: [http://localhost:9870](http://localhost:9870)

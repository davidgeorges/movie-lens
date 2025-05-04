import streamlit as st
import pandas as pd
import altair as alt
from pymongo import MongoClient
from wordcloud import WordCloud
import matplotlib.pyplot as plt

# --- CONFIGURATION DE LA PAGE ---
st.set_page_config(page_title="MovieLens", page_icon=":clapper:", layout="wide")

# --- CONNEXION MONGODB ---
@st.cache_resource
def get_mongo_client():
    return MongoClient("mongodb://mongodb:27017")

# --- RECUPERATION DES DONNEES DE MONGODB ---
@st.cache_data
def get_data_from_mongo():
    client = get_mongo_client()
    db = client["recommandation_simple_db"]
    ratings_df = pd.DataFrame(list(db["ratings_streamed"].find()))
    movies_df = pd.DataFrame(list(db["movies"].find()))

    # Renommer '_id' si nécessaire
    if '_id' in ratings_df.columns:
        ratings_df = ratings_df.rename(columns={'_id': 'mongo_id_ratings'})
    if '_id' in movies_df.columns:
        movies_df = movies_df.rename(columns={'_id': 'mongo_id_movies'})

    # Vérifications des colonnes essentielles
    if 'movieId' not in movies_df.columns:
        st.error("Erreur: La colonne 'movieId' est manquante dans la collection 'movies'.")
        return None, None
    if 'movieId' not in ratings_df.columns:
        st.error("Erreur: La colonne 'movieId' est manquante dans la collection 'ratings'.")
        return None, None
    if 'userId' not in ratings_df.columns:
        st.error("Erreur: La colonne 'userId' est manquante dans la collection 'ratings'.")
        return None, None
    if 'title' not in movies_df.columns:
        st.error("Erreur: La colonne 'title' est manquante dans la collection 'movies'.")
        return None, None
    if 'rating' not in ratings_df.columns:
        st.error("Erreur: La colonne 'rating' est manquante dans la collection 'ratings'.")
        return None, None

    return ratings_df, movies_df

ratings_df, movies_df = get_data_from_mongo()

if ratings_df is not None and movies_df is not None:
    # Fusionner les DataFrames
    data = pd.merge(ratings_df, movies_df, on='movieId')

    # --- STYLE CINÉMA ET ÉVALUATION AVEC TITRE CENTRÉ ET ICONS ---
    st.markdown(
        """
        <style>
        body {
            background-color: #e0e0e0; /* Arrière-plan gris */
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        }
        .stApp {
            background-color: #f9f9f9;
            color: #2d3e50;
            padding: 30px;
            border-radius: 15px;
            box-shadow: 0 4px 10px rgba(0,0,0,0.15);
        }
        .title-container {
            display: flex;
            justify-content: center;
            align-items: center;
            margin-bottom: 30px;
        }
        h1 {
            color: #e67e22;
            font-family: 'Impact', sans-serif;
            font-size: 5em; /* Taille encore plus grande */
            letter-spacing: 2px;
            text-shadow: 3px 3px #ccc;
            border-bottom: 5px solid #e67e22;
            padding-bottom: 15px;
            margin: 0;
        }
        h2 {
            color: #2c3e50;
            border-bottom: 2px solid #ddd;
            padding-bottom: 8px;
            margin-top: 25px;
        }
        h3 {
            color: #34495e;
        }
        .stDataFrame th, .stDataFrame td {
            color: #333;
            border-color: #eee;
        }
        .stSelectbox div > div > div > div {
            color: #333;
            background-color: #fff;
            border: 1px solid #ccc;
            border-radius: 5px;
        }
        .stButton > button {
            color: #fff;
            background-color: #e67e22;
            border: none;
            border-radius: 8px;
            padding: 10px 20px;
            cursor: pointer;
            font-weight: bold;
            transition: background-color 0.3s ease;
        }
        .stButton > button:hover {
            background-color: #d35400;
        }
        .stInfo {
            background-color: #f8f8f8;
            color: #777;
            padding: 12px;
            border-radius: 8px;
            border: 1px solid #eee;
        }
        /* Style pour les graphiques Altair */
        .vega-embed {
            width: 100% !important;
        }
        /* Style pour le top 5 des films */
        .top-movie-item {
            padding: 10px 0;
            border-bottom: 1px dashed #ccc;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .top-movie-item:last-child {
            border-bottom: none;
        }
        .top-movie-title {
            font-size: 1.1em;
            font-weight: bold;
            color: #34495e;
        }
        .top-movie-rating {
            color: #f39c12;
            font-weight: bold;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<div class='title-container'><h1>🎬 Écran Critique : Vos Films à la Loupe 🎬</h1></div>", unsafe_allow_html=True)

    st.header("🍿 Tendances Globales")

    # Top 5 des films les mieux notés (global) - Présentation stylisée avec icône
    st.subheader("🏆 Top 5 des Films les Plus Appréciés")
    top_5_global = data.groupby('title')['rating'].mean().sort_values(ascending=False).head(5).reset_index()
    top_5_global.columns = ['Titre', 'Note Moyenne']
    for index, row in top_5_global.iterrows():
        st.markdown(f"""
            <div class="top-movie-item">
                <span class="top-movie-title">{index + 1}. {row['Titre']}</span>
                <span class="top-movie-rating">Note : {row['Note Moyenne']:.2f} ⭐</span>
            </div>
        """, unsafe_allow_html=True)
    st.markdown("<hr>", unsafe_allow_html=True) # Séparateur

    col_global1, col_global2 = st.columns(2)

    with col_global1:
        st.subheader("📊 Distribution globale des notes")
        global_rating_counts = data['rating'].value_counts().sort_index().reset_index()
        global_rating_counts.columns = ['Note', 'Nombre de notes']

        chart_global_ratings = alt.Chart(global_rating_counts).mark_bar(color='#3498db').encode(
            x=alt.X('Note:O', title='Note'),
            y=alt.Y('Nombre de notes', title='Nombre de notes'),
            tooltip=['Note', 'Nombre de notes']
        ).properties(
            height=400
        ).interactive()
        st.altair_chart(chart_global_ratings, use_container_width=True)

    with col_global2:
        st.subheader("☁️ Nuage de Mots des Films les Plus Notés")
        # Calculer la note moyenne par film
        average_ratings = data.groupby('title')['rating'].mean().sort_values(ascending=False)

        # Créer un dictionnaire où la clé est le titre et la valeur est la note (pour la pondération)
        words = {title: rating for title, rating in average_ratings.items()}

        # Générer le nuage de mots
        wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(words)

        # Afficher le nuage de mots avec matplotlib
        fig, ax = plt.subplots()
        ax.imshow(wordcloud, interpolation='bilinear')
        ax.axis("off")
        st.pyplot(fig)

    st.header("👤 Focus Utilisateur")
    user_ids = data['userId'].unique()
    selected_user = st.selectbox("🎬 Choisir un spectateur :", user_ids)

    user_data = data[data['userId'] == selected_user]

    col_user1, col_user2 = st.columns(2)

    with col_user1:
        st.subheader("📝 Ses films notés")
        if not user_data.empty:
            st.dataframe(user_data[['title', 'rating']].sort_values(by='rating', ascending=False), use_container_width=True)
        else:
            st.info("Ce spectateur n'a pas encore donné son avis.")

    with col_user2:
        st.subheader("⭐ Sa distribution de notes")
        if not user_data.empty:
            user_rating_counts = user_data['rating'].value_counts().sort_index().reset_index()
            user_rating_counts.columns = ['Note', 'Nombre de notes']

            chart_user_ratings = alt.Chart(user_rating_counts).mark_bar(color='#f39c12').encode(
                x=alt.X('Note:O', title='Note'),
                y=alt.Y('Nombre de notes', title='Nombre de notes'),
                tooltip=['Note', 'Nombre de notes']
            ).properties(
                height=300
            ).interactive()
            st.altair_chart(chart_user_ratings, use_container_width=True)
        else:
            st.info("Aucune note disponible pour ce spectateur.")

else:
    st.error("Impossible de charger les données depuis MongoDB.")

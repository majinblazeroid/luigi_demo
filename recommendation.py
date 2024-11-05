import pandas as pd
import pickle
import os
from surprise import Reader, Dataset, SVD
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Database configuration (adjust this URI for your database)
DATABASE_URI = 'sqlite:///movies.db'
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
session = Session()

# Define the base and create recommendations table
Base = declarative_base()

class Recommendation(Base):
    __tablename__ = 'recommendations'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer)
    movie_list = Column(Text)  # Store movie IDs as comma-separated strings
    timestamp = Column(DateTime, default=datetime.utcnow)

# Create the table if it doesn’t exist
Base.metadata.create_all(engine)

# Load the data (loaded once when the module is imported)
users_df = pd.read_csv('data/users.csv')
user_watched_df = pd.read_csv('data/user_watched.csv')
movies_df = pd.read_csv('data/movies.csv')

# Check for and remove missing values
users_df.dropna(inplace=True)
user_watched_df.dropna(inplace=True)
movies_df.dropna(inplace=True)

# Merge DataFrames using id (pre-computed)
merged_df = user_watched_df.merge(users_df, on='user_id').merge(movies_df, left_on='movie_id', right_on='id')

# Calculate average rating and watch count for each movie (pre-computed)
movie_stats = merged_df.groupby('id').agg({
    'rating': 'mean',
    'user_id': 'count'
}).reset_index()
movie_stats.columns = ['id', 'avg_rating', 'watch_count']

# Normalize watch count (pre-computed)
movie_stats['normalized_watch_count'] = (movie_stats['watch_count'] - movie_stats['watch_count'].min()) / (movie_stats['watch_count'].max() - movie_stats['watch_count'].min())

# Calculate a combined score (pre-computed, 50% rating, 50% watch count)
movie_stats['combined_score'] = 0.5 * movie_stats['avg_rating'] + 0.5 * movie_stats['normalized_watch_count']

# Prepare data for Surprise
reader = Reader(rating_scale=(1, 5))
data = Dataset.load_from_df(merged_df[['user_id', 'id', 'rating']], reader)

def recommend_movies(user_id, n=20):
    # Check if user exists in the dataset
    if user_id not in users_df['user_id'].values:
        # For new users, recommend popular movies
        recommendations = movie_stats.nlargest(40, 'combined_score')['id']
        # Ensure we don't try to sample more items than available
        n = min(n, len(recommendations))
        recommendations = recommendations.sample(n, replace=False).tolist()
    else:
        # Get movies watched by the user
        watched_movies = user_watched_df[user_watched_df['user_id'] == user_id]['movie_id'].values

        # Filter out watched movies using a faster vectorized approach
        unwatched_movies = movie_stats[~movie_stats['id'].isin(watched_movies)]

        # Limit the number of unwatched movies for prediction to top-N candidates (e.g., based on combined score)
        top_candidates = unwatched_movies.sort_values('combined_score', ascending=False).head(100)  # Limit to top 100 for faster predictions

        # Predict ratings for top candidate movies
        predictions = []
        for movie_id in top_candidates['id']:
            predicted_rating = svd.predict(user_id, movie_id).est
            normalized_watch_count = top_candidates[top_candidates['id'] == movie_id]['normalized_watch_count'].values[0]
            combined_score = 0.5 * predicted_rating + 0.5 * normalized_watch_count
            predictions.append((movie_id, combined_score))

        # Sort predictions by combined score and get top n recommendations
        predictions.sort(key=lambda x: x[1], reverse=True)
        recommendations = [movie_id for movie_id, _ in predictions[:n]]

        new_recommendation = Recommendation(user_id=user_id, movie_list=recommendations)
        session.add(new_recommendation)
        session.commit()

    return ','.join(map(str, recommendations))
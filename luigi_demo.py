import luigi
import os
import pandas as pd
import pickle
from surprise import Reader, Dataset, SVD
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from data_handler import (
    update_user_movie_data,
    add_movie_to_database,
    add_user_to_database,
)
from recommendation import recommend_movies

from kafka import KafkaConsumer

# Task 1: Data Ingestion
class DataIngestionTask(luigi.Task):
    def output(self):
        return luigi.LocalTarget('data_ingestion_complete.txt')

    def run(self):
        # Create the KafkaConsumer
        consumer = KafkaConsumer(
            "movielog14",
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms=10000,
        )

        # Database Configuration
        DATABASE_URI = 'sqlite:///movies.db'

        # Set up SQLAlchemy engine and session
        engine = create_engine(DATABASE_URI)
        Session = sessionmaker(bind=engine)
        session = Session()

        # Process messages from Kafka
        data_count = 0
        for message in consumer:
            if data_count == 100:
                break
            message_content = message.value.decode()
            if 'recommendation request' in message_content:
                pass
            else:
                data_count += 1
                message_parts = message_content.split(',')
                movie_id, user_id = update_user_movie_data(session, message_parts)

                if movie_id and user_id:
                    add_movie_to_database(session, movie_id)
                    add_user_to_database(session, user_id)
        session.close()

        # Indicate that data ingestion is complete
        with self.output().open('w') as f:
            f.write('Data ingestion complete')

# Task 2: Model Training
class ModelTrainingTask(luigi.Task):
    def requires(self):
        return DataIngestionTask()

    def output(self):
        return luigi.LocalTarget('svd_model.pkl')

    def run(self):
        # Load data from the database
        DATABASE_URI = 'sqlite:///movies.db'
        engine = create_engine(DATABASE_URI)
        Session = sessionmaker(bind=engine)
        session = Session()

        user_watched_df = pd.read_sql_table('user_watched', engine)
        users_df = pd.read_sql_table('users', engine)
        movies_df = pd.read_sql_table('movies', engine)

        # Prepare data for Surprise
        merged_df = user_watched_df.merge(users_df, on='user_id').merge(
            movies_df, left_on='movie_id', right_on='id'
        )

        reader = Reader(rating_scale=(1, 5))
        data = Dataset.load_from_df(merged_df[['user_id', 'movie_id', 'rating']], reader)

        # Train the model
        svd = SVD()
        trainset = data.build_full_trainset()
        svd.fit(trainset)

        # Save the trained model using built-in open()
        with open(self.output().path, 'wb') as f:
            pickle.dump(svd, f)

        session.close()

# Task 3: Recommendation Generation
class RecommendationGenerationTask(luigi.Task):
    def requires(self):
        return ModelTrainingTask()

    def output(self):
        return luigi.LocalTarget('recommendations.txt')

    def run(self):
        # Load the trained model using built-in open()
        with open(self.input().path, 'rb') as f:
            svd = pickle.load(f)

        # Load data from the database
        DATABASE_URI = 'sqlite:///movies.db'
        engine = create_engine(DATABASE_URI)
        Session = sessionmaker(bind=engine)
        session = Session()

        users_df = pd.read_sql_table('users', engine)
        movies_df = pd.read_sql_table('movies', engine)

        # Generate recommendations for each user
        recommendations = {}
        for user_id in users_df['user_id'].unique():
            recommended_movies = recommend_movies(user_id, svd, movies_df)
            recommendations[user_id] = recommended_movies

        # Save recommendations to a file
        with self.output().open('w') as f:
            for user_id, movies in recommendations.items():
                f.write(f"User {user_id}: {movies}\n")

        session.close()

# Recommendation Function
def recommend_movies(user_id, svd_model, movies_df, n=10):
    # Get all movie IDs
    all_movie_ids = movies_df['id'].unique()

    # Predict ratings for all movies
    predictions = [
        (movie_id, svd_model.predict(user_id, movie_id).est)
        for movie_id in all_movie_ids
    ]

    # Sort predictions by estimated rating
    predictions.sort(key=lambda x: x[1], reverse=True)

    # Get top N recommendations
    top_n = [movie_id for movie_id, _ in predictions[:n]]

    return top_n

if __name__ == '__main__':
    luigi.run(['RecommendationGenerationTask', '--local-scheduler'])

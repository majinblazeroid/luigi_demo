import os
from datetime import datetime
from json import dumps, loads
from time import sleep
from random import randint
from kafka import KafkaConsumer, KafkaProducer
import pandas as pd
import requests
from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, Text, Table, MetaData, DateTime
from sqlalchemy.orm import sessionmaker
from kafka.errors import KafkaError
from datetime import datetime
import json

# Functions
def parse_data_request(message):
    """
    Parse the data request message and return user_id, movie_id, and mpg.
    Skips the message if it contains invalid data for conversion.
    """
    try:
        # Attempt to convert user_id to int, skip if it fails
        user_id = int(message[1].replace(" ", ""))  # Remove spaces to attempt conversion
        movie_id, mpg = message[2].replace("GET /data/m/", "").split("/")
        mpg = int(mpg.replace(".mpg", ""))
    except ValueError as e:
        print(f"Skipping message due to conversion error: {message}. Error: {e}")
        return None, None, None  # Return None to indicate a failed parse

    return user_id, movie_id, mpg

def parse_rate_request(message):
    """
    Parse the rate request message and return user_id, movie_id, and rating.
    Skips the message if it contains invalid data for conversion.
    """
    try:
        # Attempt to convert user_id to int, skip if it fails
        user_id = int(message[1].replace(" ", ""))  # Remove spaces to attempt conversion
        movie_id, rating = message[2].replace("GET /rate/", "").split("=")
        rating = int(rating)
    except ValueError as e:
        print(f"Skipping message due to conversion error: {message}. Error: {e}")
        return None, None, None  # Return None to indicate a failed parse

    return user_id, movie_id, rating


def update_user_movie_data(session, message):
    """
    Update the user_watched table with the new mpg (minute watched) or rating value
    for the given user_id and movie_id combination.
    """
    if "GET /data/m/" in message[2]:
        user_id, movie_id, mpg = parse_data_request(message)
        rating = None
    elif "GET /rate/" in message[2]:
        user_id, movie_id, rating = parse_rate_request(message)
        mpg = None
    else:
        print(f"Unexpected message format: {message[2]}")
        return None, None  # Skip processing if the message format is unknown
    
    # Skip processing if parsing failed
    if user_id is None or movie_id is None:
        print("Skipping message due to parsing failure.")
        return None, None
    
    # Check if the record exists and update accordingly
    record = session.query(user_watched_table).filter_by(user_id=user_id, movie_id=movie_id).first()

    if record:
        # Prepare update values
        update_values = {}
        if mpg is not None:
            update_values['minutes_watched'] = record.minutes_watched + 1
        if rating is not None:
            update_values['rating'] = max(record.rating, rating)
        update_values['updated_at'] = datetime.now()  # Add this line

        # Print data as a comma-separated list before updating
        print(f"Updating user_watched for user_id {user_id}, movie_id {movie_id}:", 
              ', '.join([f"{k}={v}" for k, v in update_values.items()]))
        
        # Update the record in the table
        session.execute(user_watched_table.update().where(
            (user_watched_table.c.user_id == user_id) & (user_watched_table.c.movie_id == movie_id)
        ).values(update_values))
    else:
        # Insert a new record if it doesn't exist
        new_record = {
            'user_id': user_id,
            'movie_id': movie_id,
            'minutes_watched': 1 if mpg else 0,
            'rating': rating or -1,
            'updated_at': datetime.now()  # Add this line
        }
        
        # Print data as a comma-separated list before insertion
        print("Adding user_watched:", ', '.join(f"{k}={v}" for k, v in new_record.items()))
        
        session.execute(user_watched_table.insert().values(new_record))

    # Commit changes to the database
    session.commit()
    return movie_id, user_id
    
def fetch_movie_data(movie_id):
    """
    Fetches movie data from the API.
    """
    movie_url = f"http://128.2.204.215:8080/movie/{movie_id}"
    try:
        response = requests.get(movie_url)
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except Exception as e:
        return None

def add_movie_to_database(session, movie_id):
    """
    Adds movie data to the database if it doesn't already exist.
    """
    # Check if the movie already exists in the database
    if session.query(movies_table).filter_by(id=movie_id).first():
        return

    # Fetch movie data from the API
    movie_data = fetch_movie_data(movie_id)
    if movie_data:
        # Print data as a comma-separated list before insertion for logging
        print("Adding movie:", ', '.join(str(value) for value in movie_data.values()))

        # Prepare the new row for insertion with safe type handling
        new_row = {
            'id': movie_data.get('id'),
            'tmdb_id': str(movie_data.get('tmdb_id', '')),  # Convert to string if None
            'imdb_id': str(movie_data.get('imdb_id', '')),
            'title': movie_data.get('title', ''),
            'original_title': movie_data.get('original_title', ''),
            'adult': str(movie_data.get('adult', 'False')),  # Convert to string for SQLite
            'belongs_to_collection': json.dumps(movie_data.get('belongs_to_collection', {})),  # JSON format
            'budget': movie_data.get('budget', 0),
            'genres': ', '.join([genre['name'] for genre in movie_data.get('genres', [])]),
            'homepage': movie_data.get('homepage', ''),
            'original_language': movie_data.get('original_language', ''),
            'overview': movie_data.get('overview', ''),
            'popularity': float(movie_data.get('popularity', 0.0)),
            'poster_path': movie_data.get('poster_path', ''),
            'production_companies': ', '.join(
                [company['name'] for company in movie_data.get('production_companies', [])]
            ),
            'production_countries': ', '.join(
                [country['name'] for country in movie_data.get('production_countries', [])]
            ),
            'release_date': movie_data.get('release_date', ''),
            'revenue': movie_data.get('revenue', 0),
            'runtime': movie_data.get('runtime', 0),
            'spoken_languages': ', '.join(
                [lang['name'] for lang in movie_data.get('spoken_languages', [])]
            ),
            'status': movie_data.get('status', ''),
            'vote_average': float(movie_data.get('vote_average', 0.0)),
            'vote_count': int(movie_data.get('vote_count', 0))
        }

        # Execute the insert statement
        session.execute(movies_table.insert().values(new_row))
        session.commit()

def fetch_user_data(user_id):
    """
    Fetches user data from the API.
    """
    user_url = f"http://128.2.204.215:8080/user/{user_id}"
    try:
        response = requests.get(user_url)
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except Exception as e:
        return None

def add_user_to_database(session, user_id):
    """
    Adds user data to the database if it doesn't already exist.
    """
    # Check if user already exists in the database
    existing_user = session.query(users_table).filter_by(user_id=user_id).first()
    
    if existing_user:
        print(f"User {user_id} already exists in the database.")
        return

    # Fetch user data if user doesn't exist
    user_data = fetch_user_data(user_id)
    if user_data:
        print("Adding user:", ', '.join(str(value) for value in user_data.values()))
        
        new_row = {
            'user_id': user_data.get('user_id'),
            'age': user_data.get('age'),
            'occupation': user_data.get('occupation'),
            'gender': user_data.get('gender')
        }
        
        try:
            session.execute(users_table.insert().values(new_row))
            session.commit()
            print(f"User {user_id} added to the database.")
        except sqlalchemy.exc.IntegrityError:
            # Handle duplicate entry attempt gracefully
            session.rollback()
            log_error(f"User {user_id} already exists. Skipping insertion to avoid duplicates.")
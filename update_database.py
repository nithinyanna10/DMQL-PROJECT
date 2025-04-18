import psycopg2
import os

# Connect to the database using environment variables
def connect_db():
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME"),      # Read from environment variable
        user=os.getenv("DB_USER"),        # Read from environment variable
        password=os.getenv("DB_PASSWORD"), # Read from environment variable
        host=os.getenv("DB_HOST"),        # Read from environment variable
        port=os.getenv("DB_PORT")         # Read from environment variable
    )
    return conn

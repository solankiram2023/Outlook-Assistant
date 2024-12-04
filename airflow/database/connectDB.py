import os
import time
import psycopg2
from psycopg2 import sql, Error

from services.logger import start_logger

logger = start_logger()

# Function to create connection to PostgreSQL
def create_connection_to_postgresql(attempts=3, delay=2):
    logger.info("Airflow - POSTGRESQL - database/connectDB.py - create_connection() - Creating connection to PostgreSQL database")

    # Fetch connection parameters from environment variables
    db_params = {
        "dbname": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USERNAME"),
        "password": os.getenv("DB_PASSWORD"),
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT"))
    }

    attempt = 1
    while attempt <= attempts:
        try:
            # Establish connection
            conn = psycopg2.connect(**db_params)
            logger.info("Airflow - POSTGRESQL - database/connectDB.py - create_connection() - Connection to PostgreSQL database established successfully")
            return conn
        except (Error, IOError) as e:
            if attempt == attempts:
                logger.error(f"Airflow - POSTGRESQL - database/connectDB.py - create_connection() - Failed to connect to PostgreSQL database: {e}")
                return None
            else:
                logger.warning(f"Airflow - POSTGRESQL - database/connectDB.py - create_connection() - Connection Failed: {e} - Retrying {attempt}/{attempts}")
                time.sleep(delay ** attempt)
                attempt += 1
    return None



# Function to close connection to PostgreSQL
def close_connection(dbconn, cursor=None):
    logger.info("Airflow - POSTGRESQL - database/connectDB.py - close_connection() - Closing the database connection")
    try:
        if dbconn is not None:
            if cursor is not None:
                cursor.close()
                logger.info("Airflow - POSTGRESQL - database/connectDB.py - close_connection() - Cursor closed successfully")
            dbconn.close()
            logger.info("Airflow - POSTGRESQL - database/connectDB.py - close_connection() - Connection closed successfully")
        else:
            logger.warning("Airflow - POSTGRESQL - database/connectDB.py - close_connection() - Connection does not exist")
    except Exception as e:
        logger.error(f"Airflow - POSTGRESQL - database/connectDB.py - close_connection() - Error while closing the database connection: {e}")

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import logging

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def create_connection_to_postgresql():
    """Create a connection to PostgreSQL database."""
    try:
        connection = psycopg2.connect(
            dbname=os.getenv('DB_NAME'),
            user=os.getenv('DB_USERNAME'),
            password=os.getenv('DB_PASSWORD'),
            host=os.getenv('DB_HOST'),
            port=os.getenv('DB_PORT')
        )
        return connection
    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {str(e)}")
        return None

def close_connection(connection, cursor=None):
    """Close database connection and cursor."""
    try:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
    except Exception as e:
        logger.error(f"Error closing database connection: {str(e)}")
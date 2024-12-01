from typing import Optional
from psycopg2 import connect
from utils.logs import start_logger
from psycopg2._psycopg import connection
from utils.variables import load_env_vars

# Load env
env = load_env_vars()

# Logging
logger = start_logger()

def open_connection() -> Optional[connection]:
    ''' Open a connection with PostgreSQL database and return the connection object on successful connection '''

    logger.info("DATABASE/CONNECTION - open_connection() - Opening a connection to PostgreSQL database")
    
    host = env["DATABASE_HOST"]
    port = env["DATABASE_PORT"]
    user = env["DATABASE_USER"]
    password = env["DATABASE_PASSWORD"]
    dbname = env["DATABASE_NAME"]

    try:
        # Open connection
        conn = connect(
            dbname   = dbname,
            user     = user,
            password = password,
            host     = host,
            port     = port
        )
        
        logger.info("DATABASE/CONNECTION - open_connection() - Connection established with PostgreSQL database")
        return conn 
    
    except Exception as exception:
        logger.error("DATABASE/CONNECTION - open_connection() - Failed to open a connection to PostgreSQL database (See exception below)")
        logger.error(f"DATABASE/CONNECTION - open_connection() - {exception}")

        return None
    
def close_connection(conn):
    ''' Close an open connection with PostgreSQL database '''

    try:
        if conn:
            conn.close()
            logger.info("DATABASE/CONNECTION - close_connection() - Connection closed with PostgreSQL database")
        
        else:
            logger.warning("DATABASE/CONNECTION - close_connection() - The connection object provided is not associated with any existing open connection with PostgreSQL database. Nothing to close.")
    
    except Exception as exception:
        logger.info("DATABASE/CONNECTION - close_connection() - Exception occurred while attempting to close the connection with PostgreSQL database (See exception below)")
        logger.error(f"DATABASE/CONNECTION - close_connection() - {exception}")

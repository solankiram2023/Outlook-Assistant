import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from fastapi import status, HTTPException, Depends
import json
from fastapi.responses import JSONResponse
from datetime import datetime

# Initialize Logger
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database Configuration from .env
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USERNAME"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

def connect_to_postgres():
    """
    Establishes a connection to the PostgreSQL database.
    """
    try:
        logger.info("Connecting to PostgreSQL database...")
        conn = psycopg2.connect(**DB_CONFIG)
        logger.info("Successfully connected to the database.")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to the database: {str(e)}")
        raise

def fetch_emails():
    """
    Fetches email data from the PostgreSQL database and returns a dictionary.
    """
    logger.info("SERVICES/EMAILS - fetch_emails() - Fetching emails")
    conn = connect_to_postgres()

    if conn is None:
        logger.error("SERVICES/EMAILS - fetch_emails() - Database connection failed")
        return {
            "status": status.HTTP_503_SERVICE_UNAVAILABLE,
            "message": "Database connection failed"
        }

    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        query = """
        SELECT 
            s.email_address AS sender_email,
            s.name AS sender_name,
            r.email_address AS recipient_email,
            r.email_id AS email_id,
            e.body_preview,
            e.subject,
            e.sent_datetime,
            e.received_datetime,
            e.is_read
        FROM 
            recipients r
        INNER JOIN 
            emails e ON r.email_id = e.id
        INNER JOIN 
            senders s ON e.id = s.email_id
        WHERE 
            r.email_address IN (
                SELECT 
                    email 
                FROM 
                    users
            )
        ORDER BY 
            e.received_datetime DESC;
        """
        logger.info("EMAILS - fetch_emails() - Executing SQL query")
        cursor.execute(query)
        records = cursor.fetchall()

        if not records:
            logger.info("EMAILS - fetch_emails() - No records found")
            return {
                "status": status.HTTP_404_NOT_FOUND,
                "message": "No email data found matching the query."
            }


        # Convert datetime objects to strings
        for record in records:
            if isinstance(record.get("sent_datetime"), datetime):
                record["sent_datetime"] = record["sent_datetime"].isoformat()
            if isinstance(record.get("received_datetime"), datetime):
                record["received_datetime"] = record["received_datetime"].isoformat()



        logger.info(f"EMAILS - fetch_emails() - {len(records)} records fetched successfully")
        return {
            "status": status.HTTP_200_OK,
            "data": records,
            "message": "Emails fetched successfully"
        }

    except Exception as e:
        logger.error(f"EMAILS - fetch_emails() - Error executing query: {str(e)}")
        return {
            "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
            "message": "An error occurred while fetching email data."
        }

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
        logger.info("EMAILS - fetch_emails() - Database connection closed")

def load_email(email_id: str):
    """
    Fetches email details from the database based on the provided email ID.
    """
    logger.info(f"SERVICES/EMAILS - load_email() - Loading email with ID: {email_id}")
    conn = connect_to_postgres()

    if conn is None:
        logger.error("SERVICES/EMAILS - load_email() - Database connection failed")
        return {
            "status": status.HTTP_503_SERVICE_UNAVAILABLE,
            "message": "Database connection failed"
        }

    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        query = """
        SELECT 
            s.email_address AS sender_email,
            r.name AS recipient_name,
            e.subject,
            e.received_datetime,
            e.body,
            a.name AS attachment_name
        FROM 
            emails e
        INNER JOIN 
            senders s ON e.id = s.email_id
        INNER JOIN 
            recipients r ON e.id = r.email_id
        LEFT JOIN 
            attachments a ON e.id = a.email_id AND e.has_attachments = TRUE
        WHERE 
            e.id = %s;
        """  # %s is the placeholder for email_id
        logger.info("EMAILS - load_email() - Executing SQL query")
        cursor.execute(query, (email_id,))  # Tuple for query parameter
        records = cursor.fetchall()

        if not records:
            logger.info("EMAILS - load_email() - No email found with the provided ID")
            return {
                "status": status.HTTP_404_NOT_FOUND,
                "message": "No email found with the provided ID."
            }

        # Aggregate attachments if multiple rows are returned for the same email_id
        email_data = {
            "sender_email": records[0]["sender_email"],
            "recipient_name": records[0]["recipient_name"],
            "subject": records[0]["subject"],
            "received_datetime": records[0]["received_datetime"].isoformat() if records[0]["received_datetime"] else None,
            "body": records[0]["body"],
            "attachments": [record["attachment_name"] for record in records if record["attachment_name"]]
        }

        logger.info("EMAILS - load_email() - Email data fetched successfully")
        return {
            "status": status.HTTP_200_OK,
            "data": email_data,
            "message": "Email details loaded successfully"
        }

    except Exception as e:
        logger.error(f"EMAILS - load_email() - Error executing query: {str(e)}")
        return {
            "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
            "message": "An error occurred while loading the email data."
        }

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()
        logger.info("EMAILS - load_email() - Database connection closed")

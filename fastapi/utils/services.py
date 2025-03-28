from fastapi import status
from datetime import datetime
from psycopg2.extras import RealDictCursor

from utils.logs import start_logger
from utils.variables import load_env_vars
from database.connection import open_connection, close_connection

import requests
import os

env = load_env_vars()

# Initialize Logger
logger = start_logger()

# Function to fetch emails from email folder
def fetch_emails(folder_name):
    ''' Fetches email data from the PostgreSQL database and returns a dictionary '''
    
    logger.info(f"UTILS/EMAILS - services/fetch_emails() - Fetching emails from folder {folder_name}")
    
    conn = open_connection()
    response = None

    if conn is None:
        logger.error("UTILS/EMAILS - services/fetch_emails() - Database connection failed")
        
        return {
            "status"  : status.HTTP_503_SERVICE_UNAVAILABLE,
            "message" : "Database connection failed"
        }

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            
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
                INNER JOIN
                    email_folders f ON e.parent_folder_id = f.id
                WHERE 
                    r.email_address IN (
                        SELECT 
                            email 
                        FROM 
                            users
                    )
                    AND f.display_name = %(folder_name)s
                ORDER BY 
                    e.received_datetime DESC
                LIMIT 10;
            """
            logger.info("UTILS/EMAILS - services/fetch_emails() - Executing SQL query")
            cursor.execute(query, {'folder_name': folder_name})
            records = cursor.fetchall()

            if not records:
                logger.info("UTILS/EMAILS - services/fetch_emails() - No records found")
                
                response = {
                    "status"  : status.HTTP_404_NOT_FOUND,
                    "message" : "No email data found matching the query."
                }

            # Convert datetime objects to strings
            for record in records:
                if isinstance(record.get("sent_datetime"), datetime):
                    record["sent_datetime"] = record["sent_datetime"].isoformat()
                
                if isinstance(record.get("received_datetime"), datetime):
                    record["received_datetime"] = record["received_datetime"].isoformat()


            logger.info(f"UTILS/EMAILS - services/fetch_emails() - {len(records)} records fetched successfully from {folder_name}")
            response = {
                "status"  : status.HTTP_200_OK,
                "data"    : records,
                "message" : "Emails fetched successfully"
            }

    except Exception as e:
        logger.error(f"UTILS/EMAILS - services/fetch_emails() - Error executing query: {str(e)}")
        
        response =  {
            "status"  : status.HTTP_500_INTERNAL_SERVER_ERROR,
            "message" : "An error occurred while fetching email data."
        }

    finally:
        close_connection(conn=conn)
        return response       

# Function to load email details
def load_email(email_id: str):
    ''' Fetches email details from the database based on the provided email ID '''
    
    logger.info(f"UTILS/EMAILS - load_email() - Loading email with ID: {email_id}")
    
    conn = open_connection()
    response = None

    if conn is None:
        logger.error("UTILS/EMAILS - load_email() - Database connection failed")
        
        return {
            "status"  : status.HTTP_503_SERVICE_UNAVAILABLE,
            "message" : "Database connection failed"
        }

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            
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
            """
            
            logger.info("UTILS/EMAILS - load_email() - Executing SQL query")
            cursor.execute(query, (email_id,))
            records = cursor.fetchall()

            if not records:
                logger.info("UTILS/EMAILS - load_email() - No email found with the provided ID")
                response = {
                    "status"  : status.HTTP_404_NOT_FOUND,
                    "message" : "No email found with the provided ID."
                }

            # Aggregate attachments if multiple rows are returned for the same email_id
            email_data = {
                "sender_email"      : records[0]["sender_email"],
                "recipient_name"    : records[0]["recipient_name"],
                "subject"           : records[0]["subject"],
                "received_datetime" : records[0]["received_datetime"].isoformat() if records[0]["received_datetime"] else None,
                "body"              : records[0]["body"],
                "attachments"       : [record["attachment_name"] for record in records if record["attachment_name"]]
            }

            logger.info("UTILS/EMAILS - load_email() - Email data fetched successfully")
            response =  {
                "status"  : status.HTTP_200_OK,
                "data"    : email_data,
                "message" : "Email details loaded successfully"
            }

    except Exception as e:
        logger.error(f"UTILS/EMAILS - load_email() - Error executing query: {str(e)}")
        
        response =  {
            "status"  : status.HTTP_500_INTERNAL_SERVER_ERROR,
            "message" : "An error occurred while loading the email data."
        }

    finally:
        close_connection(conn=conn)
        return response
    

def get_email_category(email_id: str):
    logger.info(f"UTILS/EMAILS - get_email_category() - Loading categories for email ID: {email_id}")
    
    conn = open_connection()
    response = None
    
    if conn is None:
        logger.error("UTILS/EMAILS - get_email_category() - Database connection failed")
        return {
            "status": status.HTTP_503_SERVICE_UNAVAILABLE,
            "message": "Database connection failed"
        }
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            
            query = """
                SELECT 
                    c.category
                FROM 
                    categories c
                WHERE 
                    c.email_id = %s
                LIMIT 3;
            """
            
            logger.info("UTILS/EMAILS - get_email_category() - Executing SQL query")
            cursor.execute(query, (email_id,))
            records = cursor.fetchall()
            
            if not records:
                logger.info("UTILS/EMAILS - get_email_category() - No categories found for the provided email ID")
                response = {
                    "status": status.HTTP_404_NOT_FOUND,
                    "message": "No categories found for the provided email ID."
                }
            else:
                # Extract categories from records
                categories = [record["category"] for record in records]
                
                logger.info("UTILS/EMAILS - get_email_category() - Categories fetched successfully")
                response = {
                    "status": status.HTTP_200_OK,
                    "data": categories,
                    "message": "Email categories loaded successfully"
                }
    
    except Exception as e:
        logger.error(f"UTILS/EMAILS - get_email_category() - Error executing query: {str(e)}")
        response = {
            "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
            "message": "An error occurred while loading the email categories."
        }
    
    finally:
        close_connection(conn=conn)
        return response
    
def get_access_token(user_email):
    logger.info(f"UTILS/EMAILS - get_access_token() - Fetching access token of user with email: {user_email}")
    
    conn = open_connection()
    response = None
    
    if conn is None:
        logger.error("UTILS/EMAILS - get_access_token() - Database connection failed")
        return {
            "status": status.HTTP_503_SERVICE_UNAVAILABLE,
            "message": "Database connection failed"
        }
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            
            query = """
                SELECT 
                    access_token
                FROM 
                    users
                WHERE 
                    email = %s
            """
            
            logger.info("UTILS/EMAILS - get_access_token() - Executing SQL query")
            cursor.execute(query, (user_email,))
            record = cursor.fetchone()
            
            if not record:
                logger.info(f"UTILS/EMAILS - get_access_token() - Access token not found for email: {user_email}")
                return None
            
            logger.info("UTILS/EMAILS - get_access_token() - Access token fetched successfully")
            return record["access_token"] 
    
    except Exception as e:
        logger.error(f"UTILS/EMAILS - get_access_token() - Error executing query: {str(e)}")
        response = {
            "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
            "message": "An error occurred while fetching access token"
        }
        return response
    
    finally:
        close_connection(conn=conn)

# Function to send an email
def send_mail_response(user_email, response_output):
    logger.info(f"UTILS/EMAILS - send_mail_response() - Sending mail response generated by response_agent")
    
    conn = open_connection()
    response = None
    
    if conn is None:
        logger.error("UTILS/EMAILS - send_mail_response() - Database connection failed")
        return {
            "status": status.HTTP_503_SERVICE_UNAVAILABLE,
            "message": "Database connection failed"
        }
    
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            
            access_token = get_access_token(user_email)

            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json"
            }

            email_body = {
                "message": {
                    "subject": response_output['subject'],
                    "body": {
                        "contentType": "HTML",
                        "content": response_output['body']
                    },
                    "toRecipients": [
                        {
                            "emailAddress": {
                                "address": response_output['recipient_email']
                            }
                        }
                    ]
                }
            }

            send_mail_endpoint = os.getenv("SEND_EMAILS_ENDPOINT")

            # Post request to send an email
            response = requests.post(
                send_mail_endpoint,
                headers=headers,
                json=email_body,
                timeout=30
            )

            if response.status_code == 202:
                logger.info(f"Email sent successfully to {response_output['recipient_email']}")
                response = {
                    "status": status.HTTP_200_OK,
                    "data": True,
                    "message": "Email sent successfully"
                }
    
    except Exception as e:
        logger.error(f"UTILS/EMAILS - send_mail_response() - Error while sending mail: {str(e)}")
        response = {
            "status": status.HTTP_500_INTERNAL_SERVER_ERROR,
            "data": False,
            "message": "An error occurred while sending an email, Failed to send an email"
        }
    
    finally:
        close_connection(conn=conn)
        return response
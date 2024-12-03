from dotenv import load_dotenv
from datetime import datetime
import psycopg2
from psycopg2 import sql, Error
import logging
import requests
import time
import os
import ast
import json
from bs4 import BeautifulSoup
import chardet
import uuid
import boto3

# Load dotenv file
load_dotenv()


# Logger function
logging.basicConfig(level = logging.INFO, format = '%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# Function to create connection to PostgreSQL
def create_connection_to_postgresql(attempts=3, delay=2):
    logger.info("Airflow - POSTGRESQL - create_connection() - Creating connection to PostgreSQL database")

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
            logger.info("Airflow - POSTGRESQL - create_connection() - Connection to PostgreSQL database established successfully")
            return conn
        except (Error, IOError) as e:
            if attempt == attempts:
                logger.error(f"Airflow - POSTGRESQL - create_connection() - Failed to connect to PostgreSQL database: {e}")
                return None
            else:
                logger.warning(f"Airflow - POSTGRESQL - create_connection() - Connection Failed: {e} - Retrying {attempt}/{attempts}")
                time.sleep(delay ** attempt)
                attempt += 1
    return None



# Function to close connection to PostgreSQL
def close_connection(dbconn, cursor=None):
    logger.info("Airflow - POSTGRESQL - close_connection() - Closing the database connection")
    try:
        if dbconn is not None:
            if cursor is not None:
                cursor.close()
                logger.info("Airflow - POSTGRESQL - close_connection() - Cursor closed successfully")
            dbconn.close()
            logger.info("Airflow - POSTGRESQL - close_connection() - Connection closed successfully")
        else:
            logger.warning("Airflow - POSTGRESQL - close_connection() - Connection does not exist")
    except Exception as e:
        logger.error(f"Airflow - POSTGRESQL - close_connection() - Error while closing the database connection: {e}")


# Function to create tables in PostgreSQL database
def create_tables_in_db():
    logger.info("Airflow - POSTGRESQL - create_tables_in_db() - Dropping the existing tables and Creating tables in PostgreSQL database")

    queries = {
        "drop_tables": {
                "drop_users_table"                  : "DROP TABLE IF EXISTS users;",
                "drop_emails_table"                 : "DROP TABLE IF EXISTS emails CASCADE;",
                "drop_recipients_table"             : "DROP TABLE IF EXISTS recipients CASCADE;",
                "drop_senders_table"                : "DROP TABLE IF EXISTS senders CASCADE;",
                "drop_attachments_table"            : "DROP TABLE IF EXISTS attachments CASCADE;",
                "drop_flags_table"                  : "DROP TABLE IF EXISTS flags CASCADE;",
                "drop_categories_table"             : "DROP TABLE IF EXISTS categories CASCADE;",
            },
        "create_tables": {
                "create_users_table": """
                    CREATE TABLE IF NOT EXISTS users (
                    id VARCHAR(255) PRIMARY KEY,
                    tenant_id VARCHAR(255),
                    name VARCHAR(255),
                    email VARCHAR(255) UNIQUE,
                    token_type VARCHAR(50),
                    access_token TEXT,
                    refresh_token TEXT,
                    id_token TEXT,
                    scope TEXT,
                    token_source VARCHAR(50),
                    issued_at TIMESTAMP,
                    expires_at TIMESTAMP,
                    nonce VARCHAR(255)
            );
                """,
                "create_emails_table": """
                CREATE TABLE IF NOT EXISTS emails (
                    body TEXT,
                    body_preview TEXT,
                    change_key VARCHAR(255),
                    content_type VARCHAR(255) DEFAULT 'html',
                    conversation_id VARCHAR(255),
                    conversation_index TEXT,
                    created_datetime TIMESTAMPTZ,
                    created_datetime_timezone VARCHAR(50),
                    end_datetime TIMESTAMPTZ,
                    end_datetime_timezone VARCHAR(50),
                    has_attachments BOOLEAN DEFAULT FALSE,
                    id VARCHAR(255) PRIMARY KEY,
                    importance VARCHAR(50),
                    inference_classification VARCHAR(50),
                    is_draft BOOLEAN DEFAULT FALSE,
                    is_read BOOLEAN,
                    is_all_day BOOLEAN,
                    is_out_of_date BOOLEAN,
                    meeting_message_type VARCHAR(255),
                    meeting_request_type VARCHAR(255),
                    odata_etag TEXT,
                    odata_value TEXT,
                    parent_folder_id VARCHAR(255),
                    received_datetime TIMESTAMPTZ,
                    recurrence TEXT,
                    reply_to TEXT,
                    response_type VARCHAR(50),
                    sent_datetime TIMESTAMPTZ,
                    start_datetime TIMESTAMPTZ,
                    start_datetime_timezone VARCHAR(50),
                    subject TEXT,
                    type VARCHAR(50),
                    web_link TEXT
                );
                """,
                "create_recipients_table": """
                CREATE TABLE IF NOT EXISTS recipients (
                    id VARCHAR(255) PRIMARY KEY,
                    email_id VARCHAR(255) REFERENCES emails(id),
                    type VARCHAR(50),
                    email_address VARCHAR(255),
                    name VARCHAR(255)
                );
                """,
                "create_senders_table": """
                CREATE TABLE IF NOT EXISTS senders (
                    id VARCHAR(255) PRIMARY KEY,
                    email_id VARCHAR(255) REFERENCES emails(id),
                    email_address VARCHAR(255),
                    name VARCHAR(255)
                );
                """,
                "create_attachments_table": """
                CREATE TABLE IF NOT EXISTS attachments (
                    id VARCHAR(255) PRIMARY KEY,
                    email_id VARCHAR(255) REFERENCES emails(id),
                    name TEXT,
                    content_type TEXT,
                    size BIGINT,
                    bucket_url TEXT
                );
                """,
                "create_flags_table": """
                CREATE TABLE IF NOT EXISTS flags (
                    email_id VARCHAR(255) PRIMARY KEY REFERENCES emails(id),
                    flag_status VARCHAR(50)
                );
                """,
                "create_categories_table": """
                    CREATE TABLE IF NOT EXISTS categories (
                        id VARCHAR(255) PRIMARY KEY,
                        email_id VARCHAR(255) REFERENCES emails(id),
                        category TEXT,
                        user_defined_category TEXT
                    );
                """,
            },
    }

    conn = create_connection_to_postgresql()

    if conn:
        try:
            cursor = conn.cursor()
            logger.info("Airflow - POSTGRESQL - create_tables_in_db() - DB Connection & cursor created successfully")

            # Execute drop table queries
            logger.info("Airflow - POSTGRESQL - create_tables_in_db() - Dropping existing tables")
            for table_name, drop_query in queries["drop_tables"].items():
                try:
                    logger.info(f"Airflow - POSTGRESQL - create_tables_in_db() - Executing drop query for table: {table_name}")
                    cursor.execute(drop_query)
                    logger.info(f"Airflow - POSTGRESQL - create_tables_in_db() - Table '{table_name}' dropped successfully.")
                except Exception as e:
                    logger.error(f"Airflow - POSTGRESQL - create_tables_in_db() - Error dropping table '{table_name}': {e}")

            # Execute create table queries
            logger.info("Airflow - POSTGRESQL - create_tables_in_db() - Creating new tables")
            for table_name, create_query in queries["create_tables"].items():
                try:
                    logger.info(f"Airflow - POSTGRESQL - create_tables_in_db() - Executing create query for table: {table_name}")
                    cursor.execute(create_query)
                    logger.info(f"Airflow - POSTGRESQL - create_tables_in_db() - Table '{table_name}' created successfully.")
                except Exception as e:
                    logger.error(f"Airflow - POSTGRESQL - create_tables_in_db() - Error creating table '{table_name}': {e}")
            
            conn.commit()
            logger.info(f"Airflow - POSTGRESQL - create_tables_in_db() - All tables dropped and created successfully")

        except Exception as e:
            logger.error(f"Airflow - POSTGRESQL - create_tables_in_db() - Error executing table queries: {e}")

        finally:
            close_connection(conn, cursor)
            logger.info(f"Airflow - POSTGRESQL - create_tables_in_db() - Connection to the DB closed")    



# Function to get token response
def get_token_response(endpoint, refresh_token):
    logging.info("Airflow - get_token_response() - Inside get_token_response() function")
    try:
        # Append the refresh token to the endpoint URL
        url = f"{endpoint}{refresh_token}"
        
        # Perform the GET request
        response = requests.get(url)
        
        # Raise an exception for HTTP errors
        response.raise_for_status()
        
        # Return the JSON response
        logger.info("Airflow - get_token_response() - Token response fetched successfully")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Airflow - get_token_response() - Error while fetching token response = {e}")


# Function to format token response
def format_token_response(token_response):
    logging.info("Airflow - format_token_response() - Inside format_token_response() function")

    # Extract the message dictionary first
    message = token_response.get("message", {})

    # Extract id_token_claims for nested data
    id_token_claims = message.get("id_token_claims", {})

    formatted_token_response = {
        "id": id_token_claims.get("oid"),
        "tenant_id": id_token_claims.get("tid"),
        "name": id_token_claims.get("name"),
        "email": id_token_claims.get("preferred_username"),
        "token_type": message.get("token_type"),
        "access_token": message.get("access_token"),
        "refresh_token": message.get("refresh_token"),
        "id_token": message.get("id_token"),
        "scope": message.get("scope"),
        "token_source": message.get("token_source"),
        "iat": datetime.utcfromtimestamp(id_token_claims.get("iat", 0)),
        "exp": datetime.utcfromtimestamp(id_token_claims.get("exp", 0)),
        "nonce": id_token_claims.get("aio"),
    }

    logging.info("Airflow - format_token_response() - Formatted the response successfully")
    return formatted_token_response


# Function to store token response with respect to user in Users table
def load_users_tokendata_to_db(formatted_token_response):
    logging.info("Airflow - load_users_tokendata_to_db() - Loading token data into USERS table")
    logging.info("Airflow - load_users_tokendata_to_db() - Creating database connection")

    conn = create_connection_to_postgresql()

    if conn:
        try:
            cursor = conn.cursor()
            insert_query = f"""
                    INSERT INTO users (
                    id, tenant_id, name, email, token_type, 
                    access_token, refresh_token, id_token, scope, 
                    token_source, issued_at, expires_at, nonce
                    ) VALUES (
                        %(id)s, %(tenant_id)s, %(name)s, %(email)s, %(token_type)s,
                        %(access_token)s, %(refresh_token)s, %(id_token)s, %(scope)s,
                        %(token_source)s, %(iat)s, %(exp)s, %(nonce)s
                    )

            """
            cursor.execute(insert_query, formatted_token_response)
            conn.commit()
            logging.info("Airflow - load_users_tokendata_to_db() - Token data inserted successfully in USERS table")

        except Exception as e:
            logger.error(f"Airflow - load_users_tokendata_to_db() - Error inserting token data into the users table = {e}")
            raise e
        finally:
            close_connection(conn, cursor)


# Function to process email JSON contents and format them
def decode_content(content):
    detected = chardet.detect(content.encode())
    encoding = detected.get('encoding') or 'utf-8'
    return content.encode().decode(encoding, errors='replace')

def clean_text(text):
    return text.replace('\n', ' ').replace('\r', '').strip()

def extract_text_and_links(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')

    # Replace <a> tags with their text and link inline (e.g., "Text (URL)")
    for a_tag in soup.find_all('a', href=True):
        link_text = a_tag.get_text(strip=True)
        href = a_tag['href']
        a_tag.replace_with(f"{link_text} ({href})")

    # Extract the cleaned text
    return soup.get_text(separator='\n', strip=True)

# Function to process the mail responses
def process_email_response(email_data):
    logging.info(f"Airflow - process_email_response() - Processing mail responses")
    formatted_email_data = {
        "@odata.context": email_data.get("@odata.context", ""),
        "value": []
    }

    logging.info(f"Airflow - process_email_response() - Parsing through each mail")
    for email in email_data.get("value", []):
        formatted_email = {}

        for key, value in email.items():
            if key == "body":
                body_content = value.get("content", "")
                cleaned_content = extract_text_and_links(body_content)

                formatted_email[key] = {
                    "contentType": value.get("contentType", "unknown"),
                    "content": clean_text(decode_content(cleaned_content))
                }

            elif isinstance(value, dict):
                # Process nested dictionaries (e.g., sender, from, toRecipients)
                formatted_email[key] = {
                    sub_key: clean_text(decode_content(str(sub_value)))
                    for sub_key, sub_value in value.items()
                }
            elif isinstance(value, list):
                # Process lists (e.g., recipients)
                formatted_email[key] = [
                    {
                        sub_key: clean_text(decode_content(str(sub_value)))
                        for sub_key, sub_value in item.items()
                    }
                    if isinstance(item, dict) else clean_text(decode_content(str(item)))
                    for item in value
                ]
            else:
                # Process other key-value pairs
                formatted_email[key] = clean_text(decode_content(str(value)))

        formatted_email_data["value"].append(formatted_email)

    logging.info(f"Airflow - process_email_response() - Data formatted successfully")
    return formatted_email_data


# Function to save mails to JSON file
def save_emails_to_json_file(email_data, file_name):
    logging.info(f"Airflow - save_emails_to_json_file() - Saving mails to JSON file {file_name}")
    try:
        with open(file_name, "w") as json_file:
            json.dump(email_data, json_file, indent=4)
        logging.info(f"Airflow - save_emails_to_json_file() - Email data saved to {file_name}")
    except Exception as e:
        logging.error(f"Airflow - save_emails_to_json_file() - Error saving email data to JSON file: {e}")


# Function to fetch mails with access token
def fetch_emails(access_token):
    logging.info("Airflow - fetch_emails() - Fetching mails from Microsoft Graph API")

    fetch_emails_url = os.getenv("FETCH_EMAILS_ENDPOINT")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Prefer": 'outlook.body-content-type="html"',
        "Content-Type": "application/json",
    }

    try:
        # Send the GET request to fetch emails
        logging.info(f"Airflow - fetch_emails() - Sending a GET request to fetch emails")
        response = requests.get(fetch_emails_url, headers=headers, timeout=30)
        response.raise_for_status()

        logging.info(f"Airflow - fetch_emails() - Emails fetched successfully")
        email_data = response.json()

        # Save the fetched emails to a JSON file
        save_emails_to_json_file(email_data, "fetch_mails.json")
        return email_data
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Airflow - fetch_emails() - Error while fetching emails : {e}")



# Function to load email data into EMAILS table
def insert_email_data(email_data):
    logging.info("Airflow - insert_email_data() - Loading email data into EMAILS table")
    logging.info("Airflow - insert_email_data() - Creating database connection")

    conn = create_connection_to_postgresql()

    if conn:
        try:
            cursor = conn.cursor()
            email_insert_query = f"""
                    INSERT INTO emails (
                    id, content_type, body, body_preview, change_key, conversation_id, conversation_index, 
                    created_datetime, created_datetime_timezone, end_datetime, end_datetime_timezone, 
                    has_attachments, importance, inference_classification, is_draft, is_read, 
                    is_all_day, is_out_of_date, meeting_message_type, meeting_request_type, 
                    odata_etag, odata_value, parent_folder_id, received_datetime, recurrence, 
                    reply_to, response_type, sent_datetime, start_datetime, start_datetime_timezone, 
                    subject, type, web_link
                ) VALUES (
                    %(id)s, %(content_type)s, %(body)s, %(body_preview)s, %(change_key)s, %(conversation_id)s, %(conversation_index)s,
                    %(created_datetime)s, %(created_datetime_timezone)s, %(end_datetime)s, %(end_datetime_timezone)s,
                    %(has_attachments)s, %(importance)s, %(inference_classification)s, %(is_draft)s, %(is_read)s,
                    %(is_all_day)s, %(is_out_of_date)s, %(meeting_message_type)s, %(meeting_request_type)s,
                    %(odata_etag)s, %(odata_value)s, %(parent_folder_id)s, %(received_datetime)s, %(recurrence)s,
                    %(reply_to)s, %(response_type)s, %(sent_datetime)s, %(start_datetime)s, %(start_datetime_timezone)s,
                    %(subject)s, %(type)s, %(web_link)s
                )
                """

            cursor.execute(email_insert_query, email_data)
            conn.commit()
            logging.info("Airflow - insert_email_data() - Email contents inserted successfully in EMAILS table")

        except Exception as e:
            logger.error(f"Airflow - insert_email_data() - Error inserting email contents into the EMAILS table = {e}")
            raise e
        finally:
            close_connection(conn, cursor)


# Function to load sender data
def insert_sender_data(sender_data):
    logging.info("Airflow - insert_sender_data() - Loading senders data into SENDERS table")
    logging.info("Airflow - insert_sender_data() - Creating database connection")

    conn = create_connection_to_postgresql()

    if conn:
        try:
            cursor = conn.cursor()
            sender_insert_query = f"""
                    INSERT INTO senders (
                        id, email_id, email_address, name
                    ) VALUES (
                        %(id)s, %(email_id)s, %(email_address)s, %(name)s
                    )
                """

            cursor.execute(sender_insert_query, sender_data)
            conn.commit()
            logging.info("Airflow - insert_sender_data() - Senders contents inserted successfully in SENDERS table")

        except Exception as e:
            logger.error(f"Airflow - insert_sender_data() - Error inserting sender contents into the SENDERS table = {e}")
            raise e
        finally:
            close_connection(conn, cursor)


# Function to load recipient data
def insert_recipient_data(recipients_data):
    logging.info("Airflow - insert_recipient_data() - Loading recipients data into RECIPIENTS table")
    logging.info("Airflow - insert_recipient_data() - Creating database connection")

    conn = create_connection_to_postgresql()

    if conn:
        try:
            cursor = conn.cursor()
            recipient_insert_query = f"""
                    INSERT INTO recipients (
                        id, email_id, type, email_address, name
                    ) VALUES (
                        %(id)s, %(email_id)s, %(type)s, %(email_address)s, %(name)s
                    )
                """

            for recipient in recipients_data:
                cursor.execute(recipient_insert_query, recipient)
            conn.commit()
            logging.info("Airflow - insert_recipient_data() - RECIPIENTS contents inserted successfully in RECIPIENTS table")

        except Exception as e:
            logger.error(f"Airflow - insert_recipient_data() - Error inserting RECIPIENTS contents into the RECIPIENTS table = {e}")
            raise e
        finally:
            close_connection(conn, cursor)


# Function to load flags data
def insert_flags_data(flags_data):
    logging.info("Airflow - insert_flags_data() - Loading flags data into FLAGS table")
    logging.info("Airflow - insert_flags_data() - Creating database connection")

    conn = create_connection_to_postgresql()

    if conn:
        try:
            cursor = conn.cursor()
            flags_insert_query = f"""
                    INSERT INTO flags (
                        email_id, flag_status
                    ) VALUES (
                        %(email_id)s, %(flag_status)s
                    )
                """

            cursor.execute(flags_insert_query, flags_data)
            conn.commit()
            logging.info("Airflow - insert_flags_data() - FLAGS contents inserted successfully in FLAGS table")

        except Exception as e:
            logger.error(f"Airflow - insert_flags_data() - Error inserting FLAGS contents into the FLAGS table = {e}")
            raise e
        finally:
            close_connection(conn, cursor)


# Function to load emails info
def load_email_info_to_db(formatted_mail_responses):

    logger.info("Airflow - load_email_info_to_db() - Loading mail information into the database")

    for email in formatted_mail_responses.get("value", []):

        email_data = {
            "id": email.get("id"),
            "content_type": email.get("body", {}).get("contentType", "html"),
            "body": email.get("body", {}).get("content", ""),
            "body_preview": email.get("bodyPreview", ""),
            "change_key": email.get("changeKey", ""),
            "conversation_id": email.get("conversationId", ""),
            "conversation_index": email.get("conversationIndex", ""),
            "created_datetime": email.get("createdDateTime", None) or None,
            "created_datetime_timezone": email.get("createdDateTime", None) or None,
            "end_datetime": email.get("endDateTime", {}).get("dateTime", None) or None,
            "end_datetime_timezone": email.get("endDateTime", {}).get("timeZone", None) or None,
            "has_attachments": email.get("hasAttachments", False),
            "importance": email.get("importance", ""),
            "inference_classification": email.get("inferenceClassification", ""),
            "is_draft": email.get("isDraft", False),
            "is_read": email.get("isRead", False),
            "is_all_day": email.get("isAllDay", False),
            "is_out_of_date": email.get("isOutOfDate", False),
            "meeting_message_type": email.get("meetingMessageType", ""),
            "meeting_request_type": email.get("meetingRequestType", ""),
            "odata_etag": email.get("@odata.etag", ""),
            "odata_value": email.get("@odata.value", ""),
            "parent_folder_id": email.get("parentFolderId", ""),
            "received_datetime": email.get("receivedDateTime", None) or None,
            "recurrence": json.dumps(email.get("recurrence", {})),
            "reply_to": json.dumps(email.get("replyTo", [])),
            "response_type": email.get("responseType", ""),
            "sent_datetime": email.get("sentDateTime", None) or None,
            "start_datetime": email.get("startDateTime", {}).get("dateTime", None) or None,
            "start_datetime_timezone": email.get("startDateTime", {}).get("timeZone", None) or None,
            "subject": email.get("subject", ""),
            "type": email.get("type", ""),
            "web_link": email.get("webLink", "")
        }

        logging.info(f"Airflow - load_email_info_to_db() - Loading mail contents to EMAILS table in database")
        insert_email_data(email_data)
        logging.info(f"Airflow - load_email_info_to_db() - Email contents uploaded to EMAILS table in database")


        sender_info = email.get("sender", {}).get("emailAddress", {})
        sender_dict = ast.literal_eval(sender_info)
        sender_data = {
            "id": str(uuid.uuid4()),
            "email_id": email.get("id", ""),
            "email_address": sender_dict.get("address", ""),
            "name": sender_dict.get("name", "")
        }
        logging.info(f"Airflow - load_email_info_to_db() - Loading sender contents to SENDERS table in database")
        insert_sender_data(sender_data)
        logging.info(f"Airflow - load_email_info_to_db() - Sender contents uploaded to SENDERS table in database")


        recipients_data = []
        for recipient_type, recipients_key in [("to", "toRecipients"), ("cc", "ccRecipients"), ("bcc", "bccRecipients")]:
            for recipient in email.get(recipients_key, []):
                recipient_info = recipient.get("emailAddress", "")
                recipient_dict = ast.literal_eval(recipient_info)
                recipients_data.append({
                    "id": str(uuid.uuid4()),
                    "email_id": email.get("id", ""),
                    "type": recipient_type,
                    "email_address": recipient_dict.get('address', ""),
                    "name": recipient_dict.get('name', "")
                })
        logging.info(f"Airflow - load_email_info_to_db() - Loading recipient contents to RECIPIENTS table in database")
        insert_recipient_data(recipients_data)
        logging.info(f"Airflow - load_email_info_to_db() - recipient contents uploaded to RECIPIENTS table in database")


        flag_data = {
            "email_id": email.get("id", ""),
            "flag_status": email.get("flag", {}).get("flagStatus","")
        }
        logging.info(f"Airflow - load_email_info_to_db() - Loading flags contents to FLAGS table in database")
        insert_flags_data(flag_data)
        logging.info(f"Airflow - load_email_info_to_db() - flags contents uploaded to FLAGS table in database")



def process_emails_with_attachments(auth, s3_bucket_name):
    """
    Process emails with attachments from the database and upload attachments to S3.
    """
    query = """
    SELECT 
        u.email AS user_email,
        e.id AS email_id,
        e.has_attachments
    FROM 
        emails e
    JOIN 
        senders s ON s.email_id = e.id
    JOIN 
        recipients r ON r.email_id = e.id
    JOIN 
        users u ON (u.email = s.email_address OR u.email = r.email_address)
    WHERE 
        e.has_attachments = TRUE;
    """

    conn = create_connection_to_postgresql()
    if conn:
        try:
            cursor = conn.cursor()
            cursor.execute(query)
            emails_with_attachments = cursor.fetchall()
            return emails_with_attachments
        except Exception as e:
            print(f"Error fetching emails: {e}")
            return []
        finally:
            close_connection(conn, cursor)
    else:
        print("Failed to connect to the database.")
        return []


def insert_attachment_data(conn, attachment_id, email_id, file_name, content_type, size, s3_url):
    """
    Insert attachment data into the attachments table.
    """
    insert_query = """
        INSERT INTO attachments (id, email_id, name, content_type, size, bucket_url)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    cursor = conn.cursor()
    try:
        cursor.execute(insert_query, (attachment_id, email_id, file_name, content_type, size, s3_url))
        conn.commit()
        logger.info(f"Attachment {file_name} inserted into the database.")
    except Exception as e:
        logger.error(f"Failed to insert attachment {file_name} into database. Error: {e}")
        conn.rollback()
    finally:
        cursor.close()

def upload_attachments_to_s3(user_email, email_id, s3_bucket_name, access_token):
    """
    Upload attachments of a given email ID to an S3 bucket and insert the attachment details into the database.
    """
    logger.info(f"Processing attachments for email ID: {email_id}")

    # Initialize S3 client
    s3_client = boto3.client("s3")

    # Fetch attachments using Microsoft Graph API
    attachment_url = f"https://graph.microsoft.com/v1.0/me/messages/{email_id}/attachments"
    response = requests.get(
        attachment_url,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )

    if response.status_code != 200:
        logger.error(f"Failed to fetch attachments for email ID: {email_id}. Response: {response.text}")
        return

    attachments = response.json().get("value", [])
    if not attachments:
        logger.info(f"No attachments found for email ID: {email_id}.")
        return

    file_extensions = {
        "JSON": [".json"],
        "Image": [".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp"],
        "CSV": [".csv"],
        "Documents": [".doc", ".docx", ".pdf", ".rtf", ".txt", ".xml"],
        "Spreadsheets": [".xls", ".xlsm", ".xlsx"],
        "Presentations": [".ppt", ".pptx", ".ppsx"],
        "Audio": [".mp3", ".wav"],
        "Video": [".mp4", ".webm"],
    }

    # Define base directory structure
    base_dir = f"{user_email}/{email_id}/attachments"

    # Create subdirectories for categorizing attachment files 
    subdirectories = {
        category: os.path.join(base_dir, category) for category in file_extensions.keys()
    }

    # Create subdirectories in S3 if they don't exist
    for sub_dir in subdirectories.values():
        s3_client.put_object(Bucket=s3_bucket_name, Key=f"{sub_dir}/")

    # Connect to the database
    conn = create_connection_to_postgresql()
    if not conn:
        logger.error("Failed to connect to the PostgreSQL database. Exiting function.")
        return

    # Upload attachments to S3 and insert data into the database
    for attachment in attachments:
        attachment_id = attachment.get("id")
        file_name = attachment.get("name")
        content_bytes = attachment.get("contentBytes")
        content_type = attachment.get("contentType")
        size = attachment.get("size")  # Assuming size is in the attachment response

        if not file_name or not content_bytes:
            continue

        # Determine the target directory based on file type
        target_dir = None
        for category, extensions in file_extensions.items():
            if any(file_name.lower().endswith(ext) for ext in extensions):
                target_dir = subdirectories.get(category)
                break

        if not target_dir:
            logger.info(f"Skipping unsupported file type: {file_name}")
            continue

        try:
            # Upload file to the S3 directory
            s3_client.put_object(
                Bucket=s3_bucket_name,
                Key=f"{target_dir}/{file_name}",
                Body=content_bytes.encode("utf-8"),
            )

            # Fetch the S3 URL for the uploaded file
            s3_url = f"s3://{s3_bucket_name}/{target_dir}/{file_name}"

            # Log the upload details
            logger.info(f"[SUCCESS] Uploaded attachment {file_name} (ID: {attachment_id}) to S3 bucket {s3_bucket_name}.")
            logger.info(f"Attachment Details: ID: {attachment_id}, Name: {file_name}, Content Type: {content_type}, Size: {size} bytes, S3 URL: {s3_url}")

            # Insert the attachment details into the database
            insert_attachment_data(conn, attachment_id, email_id, file_name, content_type, size, s3_url)

        except Exception as e:
            logger.error(f"[ERROR] Failed to upload {file_name} for email ID: {email_id}. Error: {e}")




def main():
    logger.info("Airflow - main() - Inside main function")
    # Load environment variables
    refresh_token = os.getenv("REFRESH_TOKEN")
    endpoint = os.getenv("ENDPOINT")
    s3_bucket_name = os.getenv("S3_BUCKET_NAME")

    logger.info("Airflow - main() - Calling get_token_response() function")
    token_response = get_token_response(endpoint, refresh_token)
    logger.info(f"Airflow - main() - Token Reposnse = {token_response}")

    logger.info("Airflow - main() - Calling format_token_response() function")
    formatted_token_response = format_token_response(token_response)
    logger.info(f"Airflow - main() - Formatted Token Reposnse = {formatted_token_response}")
    access_token = formatted_token_response['access_token']

    logger.info("Airflow - main() - Calling create_tables_in_db() function")
    create_tables_in_db()
    logger.info(f"Airflow - main() - Tables created successfully")

    logger.info("Airflow - main() - Calling load_users_tokendata_to_db() function")
    load_users_tokendata_to_db(formatted_token_response)
    logger.info(f"Airflow - main() - Formatted Token data with respect to user is loaded into USERS table")

    logger.info("Airflow - main() - Calling fetch_emails() function")
    mail_responses = fetch_emails(access_token)

    formatted_mail_responses = process_email_response(mail_responses)
    save_emails_to_json_file(formatted_mail_responses, "mail_responses.json")
    # print(formatted_mail_responses)

    load_email_info_to_db(formatted_mail_responses)
    # Fetch emails with attachments
    emails_with_attachments = process_emails_with_attachments(access_token, s3_bucket_name)
    logger.info("Airflow - main() - processing emails with attachments")

# Process each email's attachments
    for user_email, email_id, has_attachments in emails_with_attachments:
        if has_attachments:
            upload_attachments_to_s3(user_email, email_id, s3_bucket_name, access_token)

    logger.info("Airflow - main() - Email attachments processed and uploaded to S3 successfully")

logger.info("Airflow - main() - Workflow completed")

if __name__ == '__main__':
    main()
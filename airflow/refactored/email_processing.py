import os
import json
import requests
from utils import start_logger
from dotenv import load_dotenv
from text_processing import clean_text, decode_content, extract_text_and_links
from database import create_connection_to_postgresql, close_connection

# Load env
load_dotenv()

# Start logging
logger = start_logger()


# Function to process the mail responses
def process_email_response(email_data):
    logger.info(f"Airflow - process_email_response() - Processing mail responses")
    
    formatted_email_data = {
        "@odata.context": email_data.get("@odata.context", ""),
        "value"         : []
    }

    logger.info(f"Airflow - process_email_response() - Parsing through each mail")
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

    logger.info(f"Airflow - process_email_response() - Data formatted successfully")
    return formatted_email_data


# Function to save mails to JSON file
def save_emails_to_json_file(email_data, file_name):
    logger.info(f"Airflow - save_emails_to_json_file() - Saving mails to JSON file {file_name}")
    try:
        with open(file_name, "w") as json_file:
            json.dump(email_data, json_file, indent=4)
        logger.info(f"Airflow - save_emails_to_json_file() - Email data saved to {file_name}")
    except Exception as e:
        logger.error(f"Airflow - save_emails_to_json_file() - Error saving email data to JSON file: {e}")


# Function to fetch mails with access token
def fetch_emails(access_token):
    logger.info("Airflow - fetch_emails() - Fetching mails from Microsoft Graph API")

    fetch_emails_url = os.getenv("FETCH_EMAILS_ENDPOINT")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Prefer": 'outlook.body-content-type="html"',
        "Content-Type": "application/json",
    }

    try:
        # Send the GET request to fetch emails
        logger.info(f"Airflow - fetch_emails() - Sending a GET request to fetch emails")
        response = requests.get(fetch_emails_url, headers=headers, timeout=30)
        response.raise_for_status()

        logger.info(f"Airflow - fetch_emails() - Emails fetched successfully")
        email_data = response.json()

        # Save the fetched emails to a JSON file
        save_emails_to_json_file(email_data, "fetch_mails.json")
        return email_data
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Airflow - fetch_emails() - Error while fetching emails : {e}")


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
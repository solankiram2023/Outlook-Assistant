import os
import json
import chardet
import requests
from bs4 import BeautifulSoup
from unidecode import unidecode

from database.loadtoDB import load_email_info_to_db


# Function to fetch mails with access token
def fetch_emails(logger, access_token):
    logger.info("Airflow - services/processEmails.py - fetch_emails() - Fetching mails from Microsoft Graph API")

    fetch_emails_url = os.getenv("FETCH_EMAILS_ENDPOINT")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Prefer": 'outlook.body-content-type="html"',
        "Content-Type": "application/json",
    }

    try:
        # Send the GET request to fetch emails
        logger.info(f"Airflow - services/processEmails.py - fetch_emails() - Sending a GET request to fetch emails")
        response = requests.get(fetch_emails_url, headers=headers, timeout=30)
        response.raise_for_status()

        logger.info(f"Airflow - services/processEmails.py - fetch_emails() - Emails fetched successfully")
        email_data = response.json()

        # Save the fetched emails to a JSON file
        save_emails_to_json_file(logger, email_data, "fetch_mails.json")
        return email_data
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Airflow - services/processEmails.py - fetch_emails() - Error while fetching emails : {e}")

# Function to process email JSON contents and format them
def decode_content(content):
    return unidecode(content)

def clean_text(text):
    return text.replace('\n', ' ').replace('\r', '').strip()

def extract_text_and_links(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')

    # Replace <a> tags with their text and link inline (e.g., "Text (URL)")
    for a_tag in soup.find_all('a', href=True):
        link_text = a_tag.get_text(strip=True)
        if a_tag.get('originalsrc', None):
            href = a_tag['originalsrc']
        else:
            href = a_tag['href']
        a_tag.replace_with(f"{link_text} ({href})")

    # Extract the cleaned text
    return soup.get_text(separator='\n', strip=True)

# Function to process the mail responses
def process_email_response(logger, email_data):
    logger.info(f"Airflow - services/processEmails.py - process_email_response() - Processing mail responses")
    formatted_email_data = {
        "@odata.context": email_data.get("@odata.context", ""),
        "value": []
    }

    logger.info(f"Airflow - services/processEmails.py - process_email_response() - Parsing through each mail")
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

    logger.info(f"Airflow - services/processEmails.py - process_email_response() - Data formatted successfully")
    return formatted_email_data

# Function to save mails to JSON file
def save_emails_to_json_file(logger, email_data, file_name):
    logger.info(f"Airflow - services/processEmails.py - save_emails_to_json_file() - Saving mails to JSON file {file_name}")
    try:
        with open(file_name, "w") as json_file:
            json.dump(email_data, json_file, indent=4)
        logger.info(f"Airflow - services/processEmails.py - save_emails_to_json_file() - Email data saved to {file_name}")
    except Exception as e:
        logger.error(f"Airflow - services/processEmails.py - save_emails_to_json_file() - Error saving email data to JSON file: {e}")


def process_emails(logger, access_token):
    logger.info(f"Airflow - services/processEmails.py - process_emails() - Processing emails")

    logger.info(f"Airflow - services/processEmails.py - process_emails() - Fetching emails with access token")
    mail_responses = fetch_emails(logger, access_token)

    logger.info(f"Airflow - services/processEmails.py - process_emails() - Processing mail responses to format contents of emails")
    formatted_mail_responses = process_email_response(logger, mail_responses)
    save_emails_to_json_file(logger, formatted_mail_responses, "mail_responses.json")

    logger.info(f"Airflow - services/processEmails.py - process_emails() - Loading mail data into PostgreSQL database")
    load_email_info_to_db(logger, formatted_mail_responses)
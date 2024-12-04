import os
from utils import start_logger
from dotenv import load_dotenv

from authentication import get_token_response, format_token_response
from database import create_tables_in_db, load_users_tokendata_to_db, load_email_info_to_db
from email_processing import fetch_emails, process_email_response, save_emails_to_json_file, process_emails_with_attachments
from s3_operations import upload_attachments_to_s3

# Load env
load_dotenv()

# Start logging
logger = start_logger()


def main():
    logger.info("Airflow - main() - Inside main function")
    
    # Load environment variables
    refresh_token   = os.getenv("REFRESH_TOKEN")
    endpoint        = os.getenv("ENDPOINT")
    s3_bucket_name  = os.getenv("S3_BUCKET_NAME")

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
    user_email = load_users_tokendata_to_db(formatted_token_response)
    logger.info(f"Airflow - main() - Formatted Token data with respect to user is loaded into USERS table")

    logger.info("Airflow - main() - Calling fetch_emails() function")
    mail_responses = fetch_emails(access_token)

    formatted_mail_responses = process_email_response(mail_responses)
    save_emails_to_json_file(formatted_mail_responses, "mail_responses.json")
    # print(formatted_mail_responses)

    load_email_info_to_db(formatted_mail_responses, user_email)
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
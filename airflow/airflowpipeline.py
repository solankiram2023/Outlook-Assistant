from dotenv import load_dotenv
import os


from services.logger import start_logger
from auth.accessToken import get_token_response, format_token_response
from database.setupTables import create_tables_in_db
from database.loadtoDB import load_users_tokendata_to_db
from services.processEmails import process_emails
from services.processEmailAttachments import process_emails_with_attachments

# Load dotenv file
load_dotenv()

logger = start_logger()

def main():
    logger.info("Airflow - main() - Inside main function")
    # Load environment variables
    refresh_token = os.getenv("REFRESH_TOKEN")
    endpoint = os.getenv("ENDPOINT")
    s3_bucket_name = os.getenv("S3_BUCKET_NAME")

    logger.info("Airflow - main() - Calling get_token_response() function")
    token_response = get_token_response(logger, endpoint, refresh_token)
    logger.info(f"Airflow - main() - Token Reposnse = {token_response}")

    logger.info("Airflow - main() - Calling format_token_response() function")
    formatted_token_response = format_token_response(logger, token_response)
    logger.info(f"Airflow - main() - Formatted Token Reposnse = {formatted_token_response}")
    access_token = formatted_token_response['access_token']

    logger.info("Airflow - main() - Calling create_tables_in_db() function")
    create_tables_in_db(logger)
    logger.info(f"Airflow - main() - Tables created successfully")

    logger.info("Airflow - main() - Calling load_users_tokendata_to_db() function")
    load_users_tokendata_to_db(logger, formatted_token_response)
    logger.info(f"Airflow - main() - Formatted Token data with respect to user is loaded into USERS table")

    logger.info("Airflow - main() - Calling process_email_response() function")
    process_emails(logger, access_token)

    # Fetch emails with attachments
    logger.info("Airflow - main() - processing emails with attachments")
    process_emails_with_attachments(logger, access_token, s3_bucket_name)
    logger.info("Airflow - main() - Email attachments processed and uploaded to S3 successfully")

logger.info("Airflow - main() - Workflow completed")

if __name__ == '__main__':
    main()
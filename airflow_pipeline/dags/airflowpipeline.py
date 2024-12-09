from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import os
from dotenv import load_dotenv
from services.logger import start_logger
from auth.accessToken import get_token_response, format_token_response
from database.setupTables import create_tables_in_db
from database.loadtoDB import load_users_tokendata_to_db
from services.processEmails import process_emails
from services.processEmailAttachments import process_emails_with_attachments
from services.extractAttachments import extract_contents_from_attachments

# Initialize logger
logger = start_logger()

load_dotenv()

def get_and_format_token(**context):
    """Get and format authentication token"""
    try:
        logger.info("Task: get_and_format_token - Starting token fetch and format")
        refresh_token = os.getenv("REFRESH_TOKEN")
        endpoint = os.getenv("ENDPOINT")

        if not refresh_token or not endpoint:
            raise ValueError("Missing required environment variables")

        token_response = get_token_response(logger, endpoint, refresh_token)
        logger.info(f"Token Response received")
        formatted_token = format_token_response(logger, token_response)
        logger.info("Token Response formatted")

        context['task_instance'].xcom_push(key='formatted_token', value=formatted_token)
        return formatted_token
    except Exception as e:
        logger.error(f"Error in get_and_format_token: {e}")
        raise

def setup_database(**context):
    """Setup database tables if not already created"""
    try:
        logger.info("Task: setup_database - Starting database setup")
        
        # Get the task instance
        task_instance = context['task_instance']
        
        # Try to get the DB_SETUP state from XCom
        db_setup = task_instance.xcom_pull(key='DB_SETUP', include_prior_dates=True)
        
        # If DB_SETUP is None or False, we need to create tables
        if not db_setup:
            logger.info("Database not setup yet. Creating tables...")
            
            # Create tables
            create_tables_in_db(logger)
            
            # Set DB_SETUP to True in XCom
            task_instance.xcom_push(key='DB_SETUP', value=True)
            logger.info("Database tables created successfully and DB_SETUP set to True")
            
        else:
            logger.info("Database tables already exist, skipping table creation")

    except Exception as e:
        logger.error(f"Error in setup_database: {e}")
        context['task_instance'].xcom_push(key='DB_SETUP', value=False)
        raise

def process_user_token(**context):
    """Process user token and load to database"""
    try:
        logger.info("Task: process_user_token - Processing user token")
        
        # Get formatted token from previous task
        formatted_token = context['task_instance'].xcom_pull(task_ids='get_token_task', key='formatted_token')
        
        # Load user token data to database
        user_email = load_users_tokendata_to_db(logger, formatted_token)
        logger.info("User token data loaded to database")
        
        # Store user email in XCom for downstream tasks
        context['task_instance'].xcom_push(key='user_email', value=user_email)
        return user_email
    except Exception as e:
        logger.error(f"Error in process_user_token: {e}")
        raise

def process_email_data(**context):
    """Process email data"""
    try:
        logger.info("Task: process_email_data - Processing emails")
        
        # Get necessary data from previous tasks
        formatted_token = context['task_instance'].xcom_pull(task_ids='get_token_task', key='formatted_token')
        user_email = context['task_instance'].xcom_pull(task_ids='process_token_task', key='user_email')
        
        # Process emails
        process_emails(
            logger,
            formatted_token['access_token'],
            user_email,
            formatted_token['email'],
            formatted_token['id']
        )
        logger.info("Emails processed successfully")
    except Exception as e:
        logger.error(f"Error in process_email_data: {e}")
        raise

def process_attachments(**context):
    """Process email attachments"""
    try:
        logger.info("Task: process_attachments - Processing email attachments")
        
        # Get necessary data from previous tasks
        formatted_token = context['task_instance'].xcom_pull(task_ids='get_token_task', key='formatted_token')
        s3_bucket_name = os.getenv("S3_BUCKET_NAME")
        
        # Process email attachments
        process_emails_with_attachments(
            logger,
            formatted_token['access_token'],
            s3_bucket_name
        )
        logger.info("Email attachments processed successfully")
    except Exception as e:
        logger.error(f"Error in process_attachments: {e}")
        raise

def extract_attachment_contents(**context):
    """Extract contents from email attachments"""
    try:
        logger.info("Task: extract_attachment_contents - Extracting contents from attachments")
        
        extract_contents_from_attachments(logger)
        logger.info("Attachment contents extracted successfully")
    except Exception as e:
        logger.error(f"Error in extract_attachment_contents: {e}")
        raise


# Default arguments for our DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

# Create the DAG
with DAG(
    'outlook_pipeline',
    default_args=default_args,
    description='Pipeline to process emails and their attachments',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['email', 'processing']
) as dag:

    # Define tasks
    get_token_task = PythonOperator(
        task_id='get_token_task',
        python_callable=get_and_format_token,
        provide_context=True,
        dag=dag,
    )

    setup_db_task = PythonOperator(
        task_id='setup_db_task',
        python_callable=setup_database,
        provide_context=True,
        dag=dag,
    )

    process_token_task = PythonOperator(
        task_id='process_token_task',
        python_callable=process_user_token,
        provide_context=True,
        dag=dag,
    )

    process_emails_task = PythonOperator(
        task_id='process_emails_task',
        python_callable=process_email_data,
        provide_context=True,
        dag=dag,
    )

    process_attachments_task = PythonOperator(
        task_id='process_attachments_task',
        python_callable=process_attachments,
        provide_context=True,
        dag=dag,
    )

    extract_contents_task = PythonOperator(
        task_id='extract_contents_task',
        python_callable=extract_attachment_contents,
        provide_context=True,
        dag=dag,
    )

    # Define task dependencies
    get_token_task >> setup_db_task >> process_token_task >> process_emails_task >> process_attachments_task >> extract_contents_task

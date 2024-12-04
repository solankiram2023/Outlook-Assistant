import os
import boto3
import requests

from database.connectDB import create_connection_to_postgresql, close_connection

def fetch_emails_with_attachments(logger):
    logger.info(f"Airflow - services/processEmailAttachments.py - fetch_emails_with_attachments() - Fetching mails with attachments")

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
            logger.info(f"Airflow - services/processEmailAttachments.py - fetch_emails_with_attachments() - All the emails with attachments fetched successfully")
            return emails_with_attachments
        except Exception as e:
            logger.info(f"Airflow - services/processEmailAttachments.py - fetch_emails_with_attachments() - Error fetching emails with attachments: {e}")
            return []
        finally:
            close_connection(conn, cursor)
            logger.info(f"Airflow - services/processEmailAttachments.py - fetch_emails_with_attachments() - Connection closed successfully")
    else:
        logger.info(f"Airflow - services/processEmailAttachments.py - fetch_emails_with_attachments() - Failed to connect to the database. {e}")
        return []
    

def insert_attachment_data(logger, conn, attachment_id, email_id, file_name, content_type, size, s3_url):
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

def upload_attachments_to_s3(logger, user_email, email_id, s3_bucket_name, access_token):
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
            insert_attachment_data(logger, conn, attachment_id, email_id, file_name, content_type, size, s3_url)

        except Exception as e:
            logger.error(f"[ERROR] Failed to upload {file_name} for email ID: {email_id}. Error: {e}")

def process_emails_with_attachments(logger, access_token, s3_bucket_name):
    logger.info(f"Airflow - services/processEmailAttachments.py - process_emails_with_attachments() - Processing mails with attachments")

    logger.info(f"Airflow - services/processEmailAttachments.py - process_emails_with_attachments() - Fetching mails with attachments")
    emails_with_attachments = fetch_emails_with_attachments(logger)

    # Process each email's attachments
    for user_email, email_id, has_attachments in emails_with_attachments:
        if has_attachments:
            logger.info(f"Airflow - services/processEmailAttachments.py - process_emails_with_attachments() - Fetching mails with attachments")
            upload_attachments_to_s3(logger, user_email, email_id, s3_bucket_name, access_token)
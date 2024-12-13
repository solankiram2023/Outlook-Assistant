import os
import json
import csv
import boto3


from services.extractFileContents import parse_images, parse_csv_files, parse_word_file, parse_txt_files, parse_excel_files, parse_pdf_files
from services.processEmails import save_emails_to_json_file
from services.vectors import embed_email_attachments

# Function to create directories
def create_local_directory(logger, directory_path):
    directory_path = normalize_path(directory_path)
    
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        logger.info(f"Created directory: {directory_path}")


# Function to normalize file paths
def normalize_path(file_path):
    return os.path.normpath(file_path)

# Function to download attachments from S3
def download_attachments_from_s3(logger, user_email, email_id, s3_bucket_name):
    logger.info(f"Downloading attachments for email ID: {email_id} from S3.")
    
    s3_client = boto3.client("s3")
    base_download_dir = os.path.join(os.getcwd(), os.getenv("DOWNLOAD_DIRECTORY"), f"{user_email}/{email_id}")
    logger.info(f"Downloading attachments for email ID: {email_id} from S3. to {base_download_dir}")
    base_s3_prefix = f"{user_email}/{email_id}/attachments"
    
    try:
        response = s3_client.list_objects_v2(Bucket=s3_bucket_name, Prefix=base_s3_prefix)
        
        if "Contents" not in response:
            logger.info(f"No attachments found in S3 for email ID: {email_id}.")
            return
        
        for obj in response["Contents"]:
            key = obj["Key"]
            relative_path = key[len(base_s3_prefix):].lstrip("/")
            local_file_path = os.path.join(base_download_dir, relative_path)
            create_local_directory(logger, os.path.dirname(local_file_path))
            
            if os.path.exists(local_file_path):
                logger.info(f"File already exists locally: {local_file_path}. Skipping download.")
                continue
            
            s3_client.download_file(s3_bucket_name, key, local_file_path)
            logger.info(f"Downloaded {key} to {local_file_path}")
    
    except Exception as e:
        logger.error(f"Failed to download attachments for email ID: {email_id}. Error: {e}")


def extract_contents_from_file(logger, file_path):
    file_extension = os.path.splitext(file_path)[-1].lower()  # Get file extension
    content = ""

    file_extensions = {
        "PDFs"          : [".pdf"],
        "Images"        : [".png", ".jpg", ".jpeg"],
        "Docs"          : [".doc", ".docx"],
        "TextFiles"     : [".txt"],
        "SpreadSheets"  : [".xls", ".xlsx"],
        "CSVFiles"      : ['.csv'],
    }

    try:
        if file_extension in file_extensions["PDFs"]:
            logger.info("Parsing PDF file")
            content = parse_pdf_files(logger, file_path)
        
        elif file_extension in file_extensions["Images"]:
            logger.info("Parsing Image file")
            content = parse_images(logger, file_path)
        
        elif file_extension in file_extensions["Docs"]:
            logger.info("Parsing Document file")
            content = parse_word_file(logger, file_path)
        
        elif file_extension in file_extensions["TextFiles"]:
            logger.info("Parsing Text file")
            content = parse_txt_files(logger, file_path)
        
        elif file_extension in file_extensions["SpreadSheets"]:
            logger.info("Parsing Spreadsheet file")
            content = parse_excel_files(logger, file_path)
        
        elif file_extension in file_extensions["CSVFiles"]:
            logger.info("Parsing CSV file")
            content = parse_csv_files(logger, file_path)
        
        else:
            logger.warning(f"Unsupported file type: {file_extension}")
            content = f"Unsupported file type: {file_extension}"
    
    except Exception as e:
        content = f"Error processing file {file_path}: {str(e)}"
    
    return content


def extract_filepaths_with_attachments(logger, download_dir):
    logger.info(f"Airflow - services/extractAttachments.py - extract_filepaths_with_attachments() - Extracting files with attachments")
    
    extracted_data = []
    # Walk through the base directory
    email_ids = os.listdir(download_dir)

    for email_id in email_ids:
        # downloads/email_id
        email_dir = os.path.join(download_dir, email_id)
        
        if not os.path.isdir(email_dir):
            continue
        emails = os.listdir(email_dir)

        for email in emails:
            # downloads/email_id/mail_id
            mails_dir = os.path.join(email_dir, email)
            
            if not os.path.isdir(mails_dir):
                continue
            file_types = os.listdir(mails_dir)

            for file_type in file_types:
                # downloads/email_id/mail_id/file_type
                file_types_dir = os.path.join(mails_dir,file_type)
                
                if not os.path.isdir(file_types_dir):
                    continue
                files = os.listdir(file_types_dir)

                if files:
                    for file in files:
                        # downloads/email_id/mail_id/file_type/filename.ext
                        file_path = os.path.join(file_types_dir, file)
                        
                        logger.info(f"Airflow - services/extractAttachments.py - extract_filepaths_with_attachments() - Processing file: {file_path}")
                        content = extract_contents_from_file(logger, file_path)
                        logger.info(f"Extracted contents from {file} is {content}")

                        extracted_data.append({
                            "email_id"  : email_id,
                            "email"     : email,
                            "file_type" : file_type,
                            "file"      : file,
                            "content"   : content
                        })
                else:
                    continue
            else:
                continue
    return extracted_data

def extract_contents_from_attachments(logger):
    logger.info(f"Airflow - services/extractAttachments.py - extract_contents_from_attachments() - Extracting contents from email attachments")
    
    download_dir = os.path.join(os.getcwd(), os.getenv("DOWNLOAD_DIRECTORY"))
    
    # If no attachments were found, the DOWNLOAD_DIRECTORY won't exist
    if not os.path.isdir(download_dir):
        logger.warning(f"Airflow - services/extractAttachments.py - extract_contents_from_attachments() - No attachments were found so far")
        return

    extracted_data = extract_filepaths_with_attachments(logger, download_dir)
    
    save_emails_to_json_file(logger, extracted_data, "extracted_contents.json")
    embed_email_attachments(filename="extracted_contents.json")
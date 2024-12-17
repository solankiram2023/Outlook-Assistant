import requests
import logging
import boto3
import os
from typing import Dict, Any
from datetime import datetime
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class EmailService:
    def __init__(self):
        """Initialize EmailService with optional base URL."""
        self.base_url = os.getenv("FASTAPI_URL")
        self.s3_client = boto3.client('s3')
        logger.info(f"EmailService initialized with base URL: {self.base_url}")
    
    def fetch_emails(self, folder) -> Dict[str, Any]:
        """Fetch all emails from the API."""
        try:
            logger.info("Fetching emails from API...")
            response = requests.get(f"{self.base_url}/{os.getenv('FETCH_MAILS_ENDPOINT')}/{folder}")
            response.raise_for_status()
            data = response.json()
            logger.info(f"Successfully fetched {len(data.get('data', []))} emails")
            return data
        except requests.RequestException as e:
            logger.error(f"Error fetching emails: {str(e)}")
            return {
                "status": 500,
                "message": "Failed to fetch emails",
                "data": []
            }

    def get_s3_download_url(self, bucket_name: str, s3_key: str) -> str:
        """Generate a presigned URL for downloading the attachment."""
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': bucket_name,
                    'Key': s3_key
                },
                ExpiresIn=3600  # URL expires in 1 hour
            )
            return url
        except ClientError as e:
            logger.error(f"Error generating presigned URL: {str(e)}")
            return None

    def get_attachment_details(self, s3_url: str) -> Dict:
        """Parse S3 URL and get attachment details."""
        try:
            # Parse s3://bucket-name/key format
            parts = s3_url.replace('s3://', '').split('/', 1)
            if len(parts) != 2:
                return None
            
            bucket_name, s3_key = parts
            download_url = self.get_s3_download_url(bucket_name, s3_key)
            
            return {
                'bucket': bucket_name,
                'key': s3_key,
                'download_url': download_url,
                'filename': s3_key.split('/')[-1]  # Get the filename from the key
            }
        except Exception as e:
            logger.error(f"Error parsing S3 URL: {str(e)}")
            return None

    def load_email(self, email_id: str) -> Dict[str, Any]:
        """Load specific email details with S3 attachment information."""
        try:
            logger.info(f"Loading email details for ID: {email_id}")
            response = requests.get(f"{self.base_url}/{os.getenv('LOAD_MAILS_ENDPOINT')}/{email_id}")
            response.raise_for_status()
            data = response.json()
            
            # If email has attachments, add download URLs
            if data["status"] == 200 and data["data"].get("attachments"):
                enriched_attachments = []
                for attachment in data["data"]["attachments"]:
                    if isinstance(attachment, str):
                        # If attachment is just the S3 URL string
                        attachment_details = self.get_attachment_details(attachment)
                        if attachment_details:
                            enriched_attachments.append(attachment_details)
                    elif isinstance(attachment, dict) and attachment.get('bucket_url'):
                        # If attachment is a dictionary with bucket_url
                        attachment_details = self.get_attachment_details(attachment['bucket_url'])
                        if attachment_details:
                            attachment_details.update({
                                'name': attachment.get('name'),
                                'content_type': attachment.get('content_type'),
                                'size': attachment.get('size')
                            })
                            enriched_attachments.append(attachment_details)
                
                data["data"]["attachments"] = enriched_attachments
            
            return data
            
        except requests.RequestException as e:
            logger.error(f"Error loading email {email_id}: {str(e)}")
            return {
                "status": 500,
                "message": f"Failed to load email {email_id}",
                "data": None
            }
        
    def load_attachments(self, email_id: str) -> list:
        """Load attachments for a specific email."""
        try:
            email_response = self.load_email(email_id)
            if email_response["status"] == 200 and email_response["data"].get("attachments"):
                return email_response["data"]["attachments"]
            return []
        except Exception as e:
            logger.error(f"Error loading attachments for email {email_id}: {str(e)}")
            return []
        
    def get_email_category(self, email_id: str) -> Dict[str, Any]:
        """Load email category for a specific mail id"""
        try:
            logger.info(f"Loading email category for ID: {email_id}")
            response = requests.get(f"{self.base_url}/{os.getenv('LOAD_CATEGORY_ENDPOINT')}/{email_id}")
            response.raise_for_status()
            data = response.json()
            
            # Return the data directly since it already contains the categories in the format we want
            if data["status"] == 200 and isinstance(data.get("data"), list):
                return {
                    "status": data["status"],
                    "data": data["data"],  # This will be the list of categories
                    "message": data.get("message", "Email categories loaded successfully")
                }
            
            # If response format is unexpected, return empty list
            logger.warning(f"Unexpected response format for email categories: {data}")
            return {
                "status": 200,
                "data": [],
                "message": "No categories found"
            }
                
        except requests.RequestException as e:
            logger.error(f"Error loading categories for email {email_id}: {str(e)}")
            return {
                "status": 500,
                "message": f"Failed to load categories for email {email_id}",
                "data": []
            }
        
        
    def send_user_prompt(self, user_email: str, user_input: str, email_id) -> Dict[str, Any]:
        """Sending user prompt to chatbot"""
        try:
            logger.info(f"Sending user prompt {user_input} to chatbot")

            logger.info(f"URL for chatbot: {self.base_url}{os.getenv('CHAT_ENDPOINT')}")
            data = {
                        "user_input": user_input,
                        "user_email": user_email,
                        "email_context": {
                            "email_id": '' if email_id is None else email_id
                        }
                    }
            logger.info(f"JSON: {data}")

            # Make the POST request with JSON payload
            response = requests.post(
                url=f"{self.base_url}{os.getenv('CHAT_ENDPOINT')}",
                json=data
            )
            response.raise_for_status()
            logger.info(f"Response = {response.json()}")
            
            return response.json()
                
        except requests.RequestException as e:
            logger.error(f"An error occurred while sending the user prompt: {e}")
            return {
                "status": 500,
                "message": f"An error occurred while sending the user prompt: {e}",
                "data": []
            }
        
    def send_email(self, user_email: str, response_output):
        """Sending response mail"""
        try:
            logger.info(f"Sending response mail")

            logger.info(f"URL for chatbot: {self.base_url}{os.getenv('SEND_MAIL_ENDPOINT')}")
            data = {
                "user_email": user_email,
                "response_output": {
                    "subject": response_output["subject"],
                    "body": response_output["body"],
                    "recipient_email": response_output["recipient_email"]
                }
            }
            logger.info(f"JSON: {data}")

            # Make the POST request with JSON payload
            response = requests.post(
                url=f"{self.base_url}{os.getenv('SEND_MAIL_ENDPOINT')}",
                json=data
            )
            response.raise_for_status()
            logger.info(f"Response = {response.json()}")
            
            return response.json()
                
        except requests.RequestException as e:
            logger.error(f"An error occurred while sending the email response: {e}")
            return {
                "status": 500,
                "message": f"An error occurred while sending the email response: {e}",
                "data": []
            }
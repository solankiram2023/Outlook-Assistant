# email_service.py
import requests
import logging
from typing import Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class EmailService:
    def __init__(self, base_url: str = 'http://127.0.0.1:5000'):
        """Initialize EmailService with optional base URL."""
        self.base_url = base_url
        logger.info(f"EmailService initialized with base URL: {self.base_url}")
        
    def fetch_emails(self) -> Dict[str, Any]:
        """Fetch all emails from the API."""
        try:
            logger.info("Fetching emails from API...")
            response = requests.get(f"{self.base_url}/fetch_emails")
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

    def load_email(self, email_id: str) -> Dict[str, Any]:
        """Load specific email details."""
        try:
            logger.info(f"Loading email details for ID: {email_id}")
            response = requests.get(f"{self.base_url}/load_email/{email_id}")
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"Error loading email {email_id}: {str(e)}")
            return {
                "status": 500,
                "message": f"Failed to load email {email_id}",
                "data": None
            }

# email_service.py
import requests
import logging
import boto3
from typing import Dict, Any
from datetime import datetime
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

class EmailService:
    def __init__(self, base_url: str = 'http://127.0.0.1:5000'):
        """Initialize EmailService with optional base URL."""
        self.base_url = base_url
        self.s3_client = boto3.client('s3')
        logger.info(f"EmailService initialized with base URL: {self.base_url}")
    
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
            response = requests.get(f"{self.base_url}/load_email/{email_id}")
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
import requests
import os

from database.loadtoDB import insert_email_folders

# Function to get email folders
def get_email_folders(logger, access_token):
    logger.info("Airflow - services/processEmailFolders - get_email_folders() - Inside get_email_folders() function")

    mailfolder_endpoint = os.getenv("MAILFOLDERS_ENDPOINT")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    try:

        response = requests.get(mailfolder_endpoint, headers=headers, timeout=60)
        response.raise_for_status()
        logger.info("Airflow - services/processEmailFolders - get_email_folders() - Request successful for fetching email folders")

        emailfolders_response = response.json()
        emailfolders = emailfolders_response.get("value", [])

        formatted_emaildirs = []

        for emailfolder in emailfolders:
            formatted_emaildir = {
                "id"                        : emailfolder.get("id"),
                "display_name"              : emailfolder.get("displayName"),
                "parent_folder_id"          : emailfolder.get("parentFolderId"),
                "child_folder_count"        : emailfolder.get("childFolderCount"),
                "unread_item_count"         : emailfolder.get("unreadItemCount"),
                "total_item_count"          : emailfolder.get("totalItemCount"),
                "size_in_bytes"             : emailfolder.get("sizeInBytes"),
                "is_hidden"                 : emailfolder.get("isHidden"),
            }
            formatted_emaildirs.append(formatted_emaildir)
 
            insert_email_folders(logger, formatted_emaildir)
            logger.info("Airflow - services/processEmailFolders - get_email_folders() - Email folders data inserted into database successfully")
         
    except requests.RequestException as e:
        logger.info(f"Airflow - services/processEmailFolders - get_email_folders() - Error while inserting email folders data: {e}")
        raise
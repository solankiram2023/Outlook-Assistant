import os
import logging
from dotenv import load_dotenv

# Load env
load_dotenv()

def start_logger():
    ''' Creates and returns a logging object to log application activity '''

    # Logger configuration
    logger = logging.getLogger(__name__)
    
    # If logger already exists, return that instead of creating a new one
    if not logger.hasHandlers():
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        # Log to console (DEV only)
        if os.getenv("APP_ENV", "DEV") == "DEV":
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        # Also log to a file
        file_handler = logging.FileHandler(os.getenv("LOG_FILE", "airflow_logs.log"), mode="w")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
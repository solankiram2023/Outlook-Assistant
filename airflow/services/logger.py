import os
import logging
from dotenv import load_dotenv

# Load env
load_dotenv()

def start_logger():
    logger = logging.getLogger(__name__)

    if not logger.hasHandlers():
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        # Log to console (DEV only)
        if os.getenv("APP_ENV", "DEV") == "DEV":
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        # Log to a file
        try:
            log_file_path = os.getenv("LOG_FILE", "airflow_logs.log")
            file_handler = logging.FileHandler(log_file_path, mode="w")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            print(f"Logging to file: {log_file_path}")  # Debugging line
        except Exception as e:
            print(f"Error setting up file handler: {e}")

    return logger
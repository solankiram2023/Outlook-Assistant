import os
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def start_logger():
    """
    Configures separate log files for INFO, ERROR, and DEBUG logs.
    """
    # Create a logger with a unique name
    logger = logging.getLogger("airflow")

    # Prevent duplicate logs by ensuring handlers are added only once
    if not logger.hasHandlers():
        logger.setLevel(logging.DEBUG)  # Set the lowest level to capture all logs

        # Define log format
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        # Create separate handlers for INFO, ERROR, and DEBUG levels
        log_dir = os.getenv("LOG_DIR", "logs")
        os.makedirs(log_dir, exist_ok=True)  # Ensure the log directory exists

        # INFO log file
        info_handler = logging.FileHandler(os.path.join(log_dir, "airflow_info.log"))
        info_handler.setLevel(logging.INFO)
        info_handler.setFormatter(formatter)
        logger.addHandler(info_handler)

        # ERROR log file
        error_handler = logging.FileHandler(os.path.join(log_dir, "airflow_error.log"))
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        logger.addHandler(error_handler)

        # Console handler (optional, for DEV mode)
        if os.getenv("APP_ENV", "DEV") == "DEV":
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        logger.info(f"Logging initialized. Logs stored in directory: {log_dir}")

    # Prevent log propagation to the root logger
    logger.propagate = False

    return logger

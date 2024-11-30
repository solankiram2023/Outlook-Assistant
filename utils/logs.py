# logs.py
# Setup the logger based on env

from utils.variables import load_env_vars
import logging

def start_logger():
    ''' Creates and returns a logging object to log application activity '''

    # Read env file
    env = load_env_vars()

    # Logger configuration
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Log to console (DEV only)
    if env["APP_ENV"] == "DEV":
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    # Also log to a file
    file_handler = logging.FileHandler(env['LOG_FILE'])
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
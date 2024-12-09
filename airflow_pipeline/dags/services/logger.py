import os
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def start_logger():
    logging.basicConfig(level = logging.INFO, format = '%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    return logger
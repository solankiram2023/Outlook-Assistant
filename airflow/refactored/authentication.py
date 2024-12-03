import requests
from utils import start_logger
from dotenv import load_dotenv
from datetime import datetime

# Load env
load_dotenv()

# Start logging
logger = start_logger()

# Function to get token response
def get_token_response(endpoint, refresh_token):
    logger.info("Airflow - get_token_response() - Inside get_token_response() function")
    try:
        # Append the refresh token to the endpoint URL
        url = f"{endpoint}{refresh_token}"
        
        # Perform the GET request
        response = requests.get(url)
        
        # Raise an exception for HTTP errors
        response.raise_for_status()
        
        # Return the JSON response
        logger.info("Airflow - get_token_response() - Token response fetched successfully")
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Airflow - get_token_response() - Error while fetching token response = {e}")


# Function to format token response
def format_token_response(token_response):
    logger.info("Airflow - format_token_response() - Inside format_token_response() function")

    # Extract the message dictionary first
    message = token_response.get("message", {})

    # Extract id_token_claims for nested data
    id_token_claims = message.get("id_token_claims", {})

    formatted_token_response = {
        "id"            : id_token_claims.get("oid"),
        "tenant_id"     : id_token_claims.get("tid"),
        "name"          : id_token_claims.get("name"),
        "email"         : id_token_claims.get("preferred_username"),
        "token_type"    : message.get("token_type"),
        "access_token"  : message.get("access_token"),
        "refresh_token" : message.get("refresh_token"),
        "id_token"      : message.get("id_token"),
        "scope"         : message.get("scope"),
        "token_source"  : message.get("token_source"),
        "iat"           : datetime.utcfromtimestamp(id_token_claims.get("iat", 0)),
        "exp"           : datetime.utcfromtimestamp(id_token_claims.get("exp", 0)),
        "nonce"         : id_token_claims.get("aio"),
    }

    logger.info("Airflow - format_token_response() - Formatted the response successfully")
    return formatted_token_response

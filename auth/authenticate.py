# authenticate.py
# Requests necessary tokens from Microsoft to work with Graph API

import jwt
from httpx import Client
from fastapi import status
from urllib.parse import quote
from utils.variables import load_env_vars
from utils.logs import start_logger

# Logging
logger = start_logger()

# Load env
env = load_env_vars()

def request_auth_token():
    ''' Request authorization token from Microsoft in order to request access tokens '''
    
    logger.info("AUTH/AUTHENTICATE - request_auth_token() - Preparing URL to request authorization tokens from Microsoft")

    # Prepare the request parameters
    login_domain    = env["LOGIN_DOMAIN"]
    auth_endpoint   = env["AUTHORIZATION_ENDPOINT"]
    
    client_id   = env["CLIENT_ID"]
    tenant_id   = env["TENANT_ID"]
    
    redirect_uri    = "http://" + env["HOSTNAME"] + ":" + env["HOST_PORT"] + env["AUTHORIZATION_RESPONSE_ENDPOINT"]
    scope = env["SCOPES"]
    state = "12345"

    # URL encode the parameters
    redirect_uri_encoded = quote(string=redirect_uri, safe='')

    url = (
        login_domain + tenant_id +  auth_endpoint + '?'
        f"client_id={client_id}&"
        f"response_type=code&"
        f"redirect_uri={redirect_uri_encoded}&"
        f"response_mode=query&"
        f"scope={scope}&"
        f"state={state}"
    )

    logger.info(f"AUTH/AUTHENTICATE - request_auth_token() - Request URL contents: {url}")

    return url

def fetch_tokens(token_type: str, request_url: str, request_data: dict, request_headers: dict):
    ''' Fetch either Access tokens or Refresh tokens from Microsoft '''

    logger.info(f"AUTH/AUTHENTICATE - fetch_tokens() - Fetching {token_type} tokens from Microsoft")

    try:

        logger.info(f"AUTH/AUTHENTICATE - fetch_tokens() - Making a POST request to {request_url}")
        with Client() as client:
            response = client.post(
                url     = request_url, 
                data    = request_data, 
                headers = request_headers
            )

        if response.status_code == status.HTTP_200_OK:
            logger.info("AUTH/AUTHENTICATE - fetch_tokens() - POST request successful (200 OK response received)")
            
            auth_dict = response.json()
            id_token = auth_dict.get('id_token', None)
            
            if id_token:          
                decoded_token = jwt.decode(
                    jwt     = id_token, 
                    options = {"verify_signature": False}
                )
                auth_dict['id_token_claims'] = decoded_token
        
        else:
            logger.warning("AUTH/AUTHENTICATE - fetch_tokens() - POST request failed")

            auth_dict = None
            error_description = response.get("error_description", "Unknown error")
            logger.error("AUTH/AUTHENTICATE - fetch_tokens() - Error occurred while fetching access tokens (See details below)")
            logger.error(f"AUTH/AUTHENTICATE - fetch_tokens() - {error_description}")
        
    except Exception as exception:
        logger.error("AUTH/AUTHENTICATE - fetch_tokens() - Exception occurred while fetching access tokens (See details below)")
        logger.error(f"AUTH/AUTHENTICATE - fetch_tokens() - {exception}")
    
    finally:
        return auth_dict

def request_access_tokens(auth_code: str):
    ''' Request access token from Microsoft to connect to Graph API '''

    logger.info("AUTH/AUTHENTICATE - request_access_tokens() - Requesting access tokens from Microsoft")

    # Prepare the request parameters
    login_domain            = env["LOGIN_DOMAIN"]
    access_token_endpoint   = env["ACCESS_TOKEN_ENDPOINT"]
    
    client_id     = env["CLIENT_ID"]
    tenant_id     = env["TENANT_ID"]
    client_secret = env["CLIENT_SECRET"]
    
    redirect_uri    = "http://" + env["HOSTNAME"] + ":" + env["HOST_PORT"] + env["AUTHORIZATION_RESPONSE_ENDPOINT"]
    scope = env["SCOPES"]

    request_url = login_domain + tenant_id +  access_token_endpoint
    request_data = {
        "client_id"     : client_id,
        "scope"         : scope,
        "code"          : auth_code,
        "redirect_uri"  : redirect_uri,
        "grant_type"    : "authorization_code",
        "client_secret" : client_secret,
    }
    request_headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    logger.info(f"AUTH/AUTHENTICATE - request_access_tokens() - Request URL: {request_url}")
    logger.info(f"AUTH/AUTHENTICATE - request_access_tokens() - Request Data: {request_data}")

    return fetch_tokens(
        token_type      = "access",
        request_url     = request_url,
        request_data    = request_data,
        request_headers = request_headers
    )


def refresh_access_tokens(refresh_token: str):
    ''' Request new access token from Microsoft to connect to Graph API '''

    logger.info("AUTH/AUTHENTICATE - refresh_access_tokens() - Requesting access tokens from Microsoft")

    # Prepare the request parameters
    login_domain            = env["LOGIN_DOMAIN"]
    access_token_endpoint   = env["ACCESS_TOKEN_ENDPOINT"]
    
    client_id     = env["CLIENT_ID"]
    tenant_id     = env["TENANT_ID"]
    client_secret = env["CLIENT_SECRET"]
    
    redirect_uri    = "http://" + env["HOSTNAME"] + ":" + env["HOST_PORT"] + env["AUTHORIZATION_RESPONSE_ENDPOINT"]
    scope = env["SCOPES"]

    request_url = login_domain + tenant_id +  access_token_endpoint
    request_data = {
        "client_id"     : client_id,
        "scope"         : scope,
        "refresh_token" : refresh_token,
        "grant_type"    : "refresh_token",
        "client_secret" : client_secret,
    }
    request_headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }

    logger.info(f"AUTH/AUTHENTICATE - refresh_access_tokens() - Request URL: {request_url}")
    logger.info(f"AUTH/AUTHENTICATE - refresh_access_tokens() - Request Data: {request_data}")

    return fetch_tokens(
        token_type      = "refresh",
        request_url     = request_url,
        request_data    = request_data,
        request_headers = request_headers
    )
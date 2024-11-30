from utils.logs import start_logger
from utils.variables import load_env_vars
from fastapi import APIRouter, Request, status
from fastapi.responses import RedirectResponse, JSONResponse
from auth.authenticate import request_auth_token, request_access_tokens, refresh_access_tokens

# Logging
logger = start_logger()

# Load env
env = load_env_vars()

# Start the router
router  = APIRouter()

@router.get(
    path        = env["SIGN_IN_ENDPOINT"],
    name        = "Sign-In",
    description = "Route to sign in the user to their Outlook account",
    tags        = ["Auth"]
)
def signin():

    logger.info(f"ROUTES/AUTH - signin() - GET {env["SIGN_IN_ENDPOINT"]} Request to sign in to Microsoft account received")
    auth_url = request_auth_token()
    
    logger.info(f"ROUTES/AUTH - signin() - Redirecting to {auth_url}")
    return RedirectResponse(auth_url)


@router.get(
    path        = env['AUTHORIZATION_RESPONSE_ENDPOINT'],
    name        = "Authorization Tokens",
    description = "Route to receive authorization tokens from Microsoft",
    tags        = ["Auth"]
)
def auth_callback(request: Request):

    logger.info(f"ROUTES/AUTH - auth_callback() - GET {env['AUTHORIZATION_RESPONSE_ENDPOINT']} Authorization tokens from Microsoft received")
    
    query_params = request.query_params

    auth_code   = query_params.get('code', None)
    state       = query_params.get('state', None)
    
    if auth_code and state:
        
        # Response received from authorization endpoint
        logger.info(f"ROUTES/AUTH - auth_callback() - Attempting to request access tokens from Microsoft")
        auth_dict = request_access_tokens(auth_code=auth_code)

        if auth_dict is not None:
            logger.info(f"ROUTES/AUTH - auth_callback() - Access tokens received")
            
            return JSONResponse(
                status_code = status.HTTP_200_OK,
                content     = {
                    "status"    : status.HTTP_200_OK,
                    "type"      : "json",
                    "message"   : auth_dict
                }
            )

    logger.info(f"ROUTES/AUTH - auth_callback() - Failed to fetch access tokens")
    return JSONResponse(
        status_code = status.HTTP_500_INTERNAL_SERVER_ERROR,
        content     = {
            "status"    : status.HTTP_500_INTERNAL_SERVER_ERROR,
            "type"      : "string",
            "message"   : "Authorization tokens could not be fetched"
        }
    )


@router.get(
    path        = env['RENEW_ACCESS_TOKEN_ENDPOINT'],
    name        = "Refresh Access Tokens",
    description = "Route to fetch new access tokens from Microsoft",
    tags        = ["Auth"]
)
def renew_access_tokens(request: Request):

    logger.info(f"ROUTES/AUTH - renew_access_tokens() - GET {env['RENEW_ACCESS_TOKEN_ENDPOINT']} Request to renew access token received")

    query_params = request.query_params
    refresh_token = query_params.get('refreshToken', None)

    if not refresh_token:

        logger.info(f"ROUTES/AUTH - renew_access_tokens() - Refresh token is missing from query parameters")
        return JSONResponse(
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR,
            content     = {
                "status"    : status.HTTP_500_INTERNAL_SERVER_ERROR,
                "type"      : "string",
                "message"   : "Refresh token was expected as a query parameter, but found None (?refreshToken=ABVX...)"
            }
        )
    
    logger.info(f"ROUTES/AUTH - renew_access_tokens() - Attempting to fetch new access tokens from Microsoft")
    auth_dict = refresh_access_tokens(refresh_token=refresh_token)

    if auth_dict is not None:
        logger.info(f"ROUTES/AUTH - renew_access_tokens() - New access tokens received")
        
        return JSONResponse(
            status_code = status.HTTP_200_OK,
            content     = {
                "status"    : status.HTTP_200_OK,
                "type"      : "json",
                "message"   : auth_dict
            }
        )

    logger.info(f"ROUTES/AUTH - renew_access_tokens() - Failed to fetch new access tokens")
    return JSONResponse(
        status_code = status.HTTP_500_INTERNAL_SERVER_ERROR,
        content     = {
            "status"    : status.HTTP_500_INTERNAL_SERVER_ERROR,
            "type"      : "string",
            "message"   : "New access tokens could not be fetched"
        }
    )
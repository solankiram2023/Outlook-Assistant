from utils.logs import start_logger
from fastapi import APIRouter, status, Request
from utils.variables import load_env_vars
from fastapi.responses import JSONResponse
from auth.authenticate import refresh_access_tokens, is_token_valid
from database.jobs import dequeue_job, trigger_airflow, delete_failed_jobs, fetch_user_via_job
from utils.services import fetch_emails, load_email

# Start the router
router  = APIRouter()

# Load env
env = load_env_vars()

# Logging
logger = start_logger()

@router.get(
    path        = env["HEALTH_ENDPOINT"],
    name        = "Health",
    description = "Route to check if FastAPI is running",
    tags        = ["Core"]
)
def healthcheck():

    logger.info("ROUTES/EXTRAS - healthcheck() - GET /health request received")

    return JSONResponse(
        status_code = status.HTTP_200_OK,
        content     = {
            "status"    : status.HTTP_200_OK,
            "type"      : "string",
            "message"   : "FastAPI is up and running!"
        }
    )

@router.get(
    path        = env["DISPATCH_ENDPOINT"],
    name        = "Dispatch Jobs",
    description = f"Dispatch Jobs that are marked as {env['DEFAULT_JOB_STATUS']}",
    tags        = ["Jobs"]
)
def dispatch_pending_jobs():
    ''' Manually trigger Airflow  '''

    logger.info("ROUTES/EXTRAS - healthcheck() - GET /health request received")

    job_id = None
    dispatch_status = None

    # First, clear all jobs marked as failed (Optional)
    delete_failed_jobs()

    # Pull a pending job from the queued jobs
    job_id = dequeue_job()

    if job_id:
        # Fetch user's data based on job_id
        auth_dict = fetch_user_via_job(job_id=int(job_id))

        if auth_dict:
            # Validate if tokens have expired or not
            is_valid = is_token_valid(auth_dict=auth_dict)
        
        if is_valid:
            # Trigger Airflow 
            dispatch_status = trigger_airflow(job_id=int(job_id))
        
        else:
            # Access token has expired
            # Attempt to regenerate the access tokens
            refresh_auth_dict = refresh_access_tokens(refresh_token=str(auth_dict["refresh_token"]))

            if refresh_auth_dict:
                dispatch_status = trigger_airflow(job_id=int(job_id))


    if job_id and dispatch_status:

        return JSONResponse(
            status_code = status.HTTP_200_OK,
            content     = {
                "status"    : status.HTTP_200_OK,
                "type"      : "string",
                "message"   : f"Dispatched job {job_id} successfully!"
            }
        )
    
    return JSONResponse(
        status_code = status.HTTP_200_OK,
        content     = {
            "status"    : status.HTTP_500_INTERNAL_SERVER_ERROR,
            "type"      : "string",
            "message"   : "Either no jobs are pending or failed to dispatch job"
        }
    )


# Router to fetch emails
@router.get(
    path        = env["FETCH_MAILS_ENDPOINT"] + "/{folder_name}",
    name        = "Fetch Emails",
    description = "Endpoint to fetch emails with sender email, body preview, and subject",
    tags        = ["Emails"]
)
def fetch_emails_endpoint(folder_name: str):

    logger.info(f"ROUTES/EMAILS - fetch_emails_endpoint() - GET /fetch_emails/{folder_name} Request to fetch email data received")

    response = fetch_emails(folder_name)  

    return JSONResponse(
        status_code = response["status"],
        content     = response
    )


# Router to load an email
@router.get(
    path        = env["LOAD_MAILS_ENDPOINT"] + "/{email_id}",
    name        = "Load Email",
    description = "Endpoint to load email details by email ID",
    tags        = ["Emails"]
)
def load_email_endpoint(email_id: str):

    logger.info(f"ROUTES/EMAILS - load_email_endpoint() - GET /load_email/{email_id} Request to load email details")

    response = load_email(email_id)

    # Return the dictionary as a JSONResponse
    return JSONResponse(
        status_code = response["status"],
        content     = response
    )

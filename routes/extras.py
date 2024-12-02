from utils.logs import start_logger
from fastapi import APIRouter, status
from utils.variables import load_env_vars
from fastapi.responses import JSONResponse
from auth.authenticate import refresh_access_tokens, is_token_valid
from database.jobs import dequeue_job, trigger_airflow, delete_failed_jobs, fetch_user_via_job

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
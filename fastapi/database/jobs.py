import json
import requests
from fastapi import status
from datetime import datetime
from utils.logs import start_logger
from requests.auth import HTTPBasicAuth
from utils.variables import load_env_vars
from database.connection import open_connection, close_connection

# Load env
env = load_env_vars()

# Logging
logger = start_logger()

def add_to_queued_jobs(email:str) -> bool:
    ''' Creates a new job to ensure user tokens are sent to Airflow via API '''

    logger.info(f"DATABASE/JOBS - add_to_queued_jobs() - Adding {email} to queued jobs")

    # Start a connection
    conn = open_connection()

    # Job added status
    job_id = None

    if conn:
        try:

            with conn.cursor() as cursor:
                logger.info(f"DATABASE/JOBS - add_to_queued_jobs() - Preparing SQL query to queued jobs...")

                # Upon successful insert, the 'id' and 'created_at' will be returned
                query = """
                    INSERT INTO queued_jobs (email, status)
                    VALUES (%s, %s)
                    RETURNING id, created_at;
                """
                logger.info(f"DATABASE/JOBS - add_to_queued_jobs() - Inserting record to queued jobs...")

                # Because it is a new job, status will always be set to 'pending'
                cursor.execute(query, (email, env['DEFAULT_JOB_STATUS']))

                # If insertion fails, this will throw an exception
                job_id, created_at = cursor.fetchone()

                conn.commit()

                logger.info(f"DATABASE/JOBS - add_to_queued_jobs() - Successfully added {email} with job_id {job_id} to queued jobs created at {created_at}")

        except Exception as exception:
            logger.error(f"DATABASE/AUTHSTORAGE - add_to_queued_jobs() - Failed to add job to the queue (See exception below)")
            logger.error(f"DATABASE/AUTHSTORAGE - add_to_queued_jobs() - {exception}")

            # If insertion failed, rollback the database to the previous state
            job_id = None
            if conn:
                conn.rollback()
        
        finally:
            close_connection(conn=conn)

    return job_id

def delete_job(job_id: int):
    ''' Delete job based on job_id '''
    
    logger.info(f"DATABASE/JOBS - delete_job() - Removing {job_id} from queued jobs")

    # Start a connection
    conn = open_connection()

    if conn:
        try:
            
            with conn.cursor() as cursor:
                logger.info(f"DATABASE/JOBS - delete_job() - Preparing SQL query to remove job...")
                
                query = """
                    DELETE FROM queued_jobs
                    WHERE id = %s
                """
                
                logger.info(f"DATABASE/JOBS - delete_job() - Removing job...")
                
                cursor.execute(query, (job_id,))
                conn.commit()

        except Exception as exception:
            logger.error(f"DATABASE/JOBS - delete_job() - Failed to delete job from the queue (See exception below)")
            logger.error(f"DATABASE/JOBS - delete_job() - {exception}")

            # Because deleting this job failed, we'll attempt to mark it as failed to ignore future processing
            update_job(job_id=job_id, status=env['JOB_FAILED'])

        finally:
            close_connection(conn=conn)


def fetch_user_via_job(job_id: int):
    ''' Fetches user data based on job_id '''

    logger.info(f"DATABASE/JOBS - fetch_job() - Fetching user data for job_id {job_id} from queued jobs")

    # Start a connection
    conn = open_connection()

    # Fetched job result
    result = None

    if conn:
        try:
            
            with conn.cursor() as cursor:
                logger.info(f"DATABASE/JOBS - fetch_job() - Preparing SQL query to fetch job...")

                query = """
                    SELECT * FROM users WHERE email IN (
                        SELECT email FROM queued_jobs
                        WHERE id = %s AND status = %s LIMIT 1
                    );
                """
                logger.info(f"DATABASE/JOBS - fetch_job() - Fetching user data via job_id...")
                
                cursor.execute(query, (job_id, env['DEFAULT_JOB_STATUS']))
                result = cursor.fetchone()

                if result:
                    # Get column names
                    column_names = [desc[0] for desc in cursor.description]

                    # Create a dictionary from columns and values
                    auth_dict = dict(zip(column_names, result))
                    result = auth_dict

                    logger.info(f"DATABASE/JOBS - fetch_job() - Fetched user data via job_id")

        except Exception as exception:
            logger.error(f"DATABASE/JOBS - delete_job() - Failed to delete job from the queue (See exception below)")
            logger.error(f"DATABASE/JOBS - delete_job() - {exception}")
        
        finally:
            close_connection(conn=conn)

    return result

def update_job(job_id: int, status:str):
    ''' Update job status based on job_id '''

    logger.info(f"DATABASE/JOBS - update_job() - Updating status for job_id {job_id} to {status}")

    # Start a connection
    conn = open_connection()

    if conn:
        try:

            with conn.cursor() as cursor:
                logger.info(f"DATABASE/JOBS - update_job() - Preparing SQL query to fetch job...")

                query = """
                    UPDATE queued_jobs
                    SET status = %s
                    WHERE id = %s;
                """
                logger.info(f"DATABASE/JOBS - update_job() - Updating status for job_id...")
                
                cursor.execute(query, (status, job_id))
                conn.commit()

                if cursor.rowcount > 0:
                    logger.info(f"DATABASE/JOBS - update_job() - Successfully updated status for job_id {job_id}")

                else:
                    logger.error(f"DATABASE/JOBS - update_job() - job_id {job_id} not found or status is already '{status}'")
        
        except Exception as exception:
            logger.error(f"DATABASE/JOBS - update_job() - Failed to update the status for job_id {job_id} (See exception below)")
            logger.error(f"DATABASE/JOBS - update_job() - {exception}") 

        finally:
            close_connection(conn=conn)

def trigger_airflow(job_id: int):
    ''' Send user token to Airflow via API and trigger the DAG '''

    logger.info(f"DATABASE/JOBS - trigger_airflow() - Triggering Airflow for job_id {job_id} from queued jobs")
    data_dict = fetch_user_via_job(job_id=job_id)
    dispatch_status = False
    
    if data_dict:
        payload = {
            "conf": data_dict
        }

        airflow_endpoint = "http://" + env['AIRFLOW_HOST'] + ':' +env['AIRFLOW_PORT'] + f"/api/v1/dags/{env['AIRFLOW_DAG_ID']}/dagRuns"
        auth = HTTPBasicAuth(username=env['AIRFLOW_USER'], password=env['AIRFLOW_PASSWORD'])

        try:
            logger.info(f"DATABASE/JOBS - trigger_airflow() - Sending user data to {airflow_endpoint}")

            # Convert datetime objects to ISO format timestamp strings (Closure)
            def serialize_datetime(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                
                raise TypeError("Type not serializable")
            
            headers = {"Content-Type": "application/json"}
            response = requests.post(
                url     = airflow_endpoint, 
                data    = json.dumps(payload, default=serialize_datetime), 
                auth    = auth,
                headers = headers
            )
            
            if response.status_code == status.HTTP_200_OK:
                logger.info(f"DATABASE/JOBS - trigger_airflow() - Successfully sent data to Airflow")
                update_job(job_id=job_id, status=env['JOB_SUCCESSFUL'])
                dispatch_status = True
            
            else:
                logger.info(f"DATABASE/JOBS - trigger_airflow() - Failed to send data to Airflow (See error below)")
                logger.info(f"DATABASE/JOBS - trigger_airflow() - {response.text}")
        
        except Exception as exception:
            logger.error(f"DATABASE/JOBS - trigger_airflow() - Exception occurred while attempting to send user data to Airflow")
            logger.error(f"DATABASE/JOBS - trigger_airflow() - {exception}")
    
    return dispatch_status

def dequeue_job():
    ''' Fetch the topmost job marked as 'pending' '''

    logger.info(f"DATABASE/JOBS - dequeue_job() - Fetching first job_id marked as {env['DEFAULT_JOB_STATUS']}")

    # Start a connection
    conn = open_connection()

    # Fetched job result
    result = None

    if conn:
        try:
            
            with conn.cursor() as cursor:
                logger.info(f"DATABASE/JOBS - fetch_job() - Preparing SQL query to fetch first job...")

                query = """
                    SELECT id FROM queued_jobs
                    WHERE status = %s ORDER BY id ASC LIMIT 1
                """
                logger.info(f"DATABASE/JOBS - fetch_job() - Fetching first job...")
                
                cursor.execute(query, (env['DEFAULT_JOB_STATUS'],))
                result = cursor.fetchone()

                if result:
                    result = result[0]
                
                else:
                    logger.warning(f"DATABASE/JOBS - dequeue_job() - No pending jobs found")
                    result = None


        except Exception as exception:
            logger.error(f"DATABASE/JOBS - delete_job() - Failed to fetch first job_id from the queue (See exception below)")
            logger.error(f"DATABASE/JOBS - delete_job() - {exception}")
        
        finally:
            close_connection(conn=conn)

    return result

def delete_failed_jobs():
    """ Delete jobs from the queued_jobs table where status = 'failed' """
    
    logger.info(f"DATABASE/JOBS - delete_failed_jobs() - Deleting jobs with status 'failed'")

    # Start a connection
    conn = open_connection()

    if conn:
        try:
            
            with conn.cursor() as cursor:
                logger.info(f"DATABASE/JOBS - delete_failed_jobs() - Preparing SQL query to delete failed jobs...")

                query = """
                    DELETE FROM queued_jobs
                    WHERE status = %s
                """
                logger.info(f"DATABASE/JOBS - delete_failed_jobs() - Deleting failed jobs...")
                
                cursor.execute(query, (env['JOB_FAILED'],))
                conn.commit()

                # Log how many rows were deleted
                rows_deleted = cursor.rowcount
                logger.info(f"DATABASE/JOBS - delete_failed_jobs() - {rows_deleted} job(s) deleted with status 'failed'")

        except Exception as exception:
            logger.error(f"DATABASE/JOBS - delete_failed_jobs() - Failed to delete failed jobs (See exception below)")
            logger.error(f"DATABASE/JOBS - delete_failed_jobs() - {exception}")
        
        finally:
            close_connection(conn=conn)

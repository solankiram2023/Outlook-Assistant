from typing import Optional
from utils.logs import start_logger
from psycopg2.extras import DictCursor
from utils.variables import load_env_vars
from database.jobs import add_to_queued_jobs, delete_job, trigger_airflow
from database.connection import open_connection, close_connection

# Load env
env = load_env_vars()

# Logging
logger = start_logger()


def check_email_exists(email):
    ''' Checks if the given email exists in the 'users' table. '''
    
    logger.info(f"DATABASE/AUTHSTORAGE - check_email_exists() - Checking if email {email} exists in the 'users' table")

    # Start a connection
    conn = open_connection()

    # Email exists status
    exists = False
    
    try:
        with conn.cursor() as cursor:
            # SQL query to check if the email exists
            sql_query = """
                SELECT email
                FROM users
                WHERE email = %s
                LIMIT 1;
            """
            logger.info("DATABASE/AUTHSTORAGE - check_email_exists() - Executing SQL query to check if user exists...")
            cursor.execute(sql_query, (email,))

            # Check if the email exists
            if cursor.fetchone():
                logger.info(f"DATABASE/AUTHSTORAGE - check_email_exists() - Email {email} exists in the 'users' table")
                exists = True
            else:
                logger.info(f"DATABASE/AUTHSTORAGE - check_email_exists() - Email {email} does not exist in the 'users' table")

    except Exception as exception:
        logger.error(f"DATABASE/AUTHSTORAGE - check_email_exists() - Failed to check email existence (See exception below)")
        logger.error(f"DATABASE/AUTHSTORAGE - check_email_exists() - {exception}")

    finally:
        close_connection(conn=conn)

    return exists


def save_auth_response(auth_dict):
    ''' Stores or updates authentication data in the 'users' table. '''

    logger.info("DATABASE/AUTHSTORAGE - save_auth_response() - Saving tokens and user data to 'users' table")

    # Start a connection
    conn = open_connection()

    # Storage status
    status = False

    # Job status
    job_id = None

    if conn:
        try:
            logger.info("DATABASE/AUTHSTORAGE - save_auth_response() - Preparing SQL query to save data...")

            id_token_claims = auth_dict.get("id_token_claims", {})
            email = id_token_claims.get("email")
            
            if not id_token_claims:
                logger.info("DATABASE/AUTHSTORAGE - save_auth_response() - id_token_claims is missing. Tokens and user data will not be saved...")
                raise ValueError("id_token_claims seems to be missing")

            # Prepare data for insertion or update
            user_data = {
                "id"                 : id_token_claims.get("oid"),
                "tenant_id"          : id_token_claims.get("tid"),
                "name"               : id_token_claims.get("name"),
                "email"              : email,
                "token_type"         : auth_dict.get("token_type"),
                "access_token"       : auth_dict.get("access_token"),
                "refresh_token"      : auth_dict.get("refresh_token"),
                "id_token"           : auth_dict.get("id_token"),
                "scope"              : auth_dict.get("scope"),
                "token_source"       : auth_dict.get("token_source"),
                "issued_at"          : id_token_claims.get("iat"),
                "expires_at"         : id_token_claims.get("exp"),
                "preferred_username" : id_token_claims.get("preferred_username"),
                "nonce"              : id_token_claims.get("nonce", "random_value"),
                "is_active"          : True
            }

            # Before inserting, check if the user is signin up for the first time
            # If true, add this user to the queued_jobs table
            # If false, do nothing and proceed
            
            user_exists = check_email_exists(email=email)
            if not user_exists:
                job_id = add_to_queued_jobs(email=email)

            # Get a cursor to load dictionary
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                
                # Insert or update into the 'users' table
                sql = """
                INSERT INTO users (id, tenant_id, name, email, token_type, access_token, refresh_token, id_token, 
                                scope, token_source, issued_at, expires_at, preferred_username, nonce, is_active)
                VALUES (%(id)s, %(tenant_id)s, %(name)s, %(email)s, %(token_type)s, %(access_token)s, 
                        %(refresh_token)s, %(id_token)s, %(scope)s, %(token_source)s, 
                        to_timestamp(%(issued_at)s), to_timestamp(%(expires_at)s), 
                        %(preferred_username)s, %(nonce)s, %(is_active)s)
                ON CONFLICT (email)
                DO UPDATE SET
                    id = EXCLUDED.id,
                    tenant_id = EXCLUDED.tenant_id,
                    name = EXCLUDED.name,
                    token_type = EXCLUDED.token_type,
                    access_token = EXCLUDED.access_token,
                    refresh_token = EXCLUDED.refresh_token,
                    id_token = EXCLUDED.id_token,
                    scope = EXCLUDED.scope,
                    token_source = EXCLUDED.token_source,
                    issued_at = EXCLUDED.issued_at,
                    expires_at = EXCLUDED.expires_at,
                    preferred_username = EXCLUDED.preferred_username,
                    nonce = EXCLUDED.nonce,
                    is_active = EXCLUDED.is_active;
                """

                logger.info("DATABASE/AUTHSTORAGE - save_auth_response() - Executing SQL query to save data...")
                
                cursor.execute(sql, user_data)
                conn.commit()

                logger.info("DATABASE/AUTHSTORAGE - save_auth_response() - Successfully saved tokens and user data to 'users' table")
                status = True
        
        except Exception as exception:
            logger.error("DATABASE/AUTHSTORAGE - save_auth_response() - Failed to save tokens and user data to 'users' table (See exception below)")
            logger.error(f"DATABASE/AUTHSTORAGE - save_auth_response() - {exception}")

            # If a job was created, but the insertion to users table failed, 
            # remove the job_id from the queued_jobs table.
            if job_id:
                delete_job(job_id=job_id)
                job_id = None
        
        finally:
            close_connection(conn=conn)

    if job_id:
        trigger_airflow(job_id=int(job_id))

    return status

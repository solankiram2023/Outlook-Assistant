import ast
import uuid
import json

from database.connectDB import create_connection_to_postgresql, close_connection

# Function to store token response with respect to user in Users table
def load_users_tokendata_to_db(logger, formatted_token_response):
    logger.info("Airflow - database/loadtoDB.py - load_users_tokendata_to_db() - Loading token data into USERS table")
    logger.info("Airflow - database/loadtoDB.py -  load_users_tokendata_to_db() - Creating database connection")

    conn = create_connection_to_postgresql()

    if conn:
        try:
            cursor = conn.cursor()
            insert_query = f"""
                    INSERT INTO users (
                    id, tenant_id, name, email, token_type, 
                    access_token, refresh_token, id_token, scope, 
                    token_source, issued_at, expires_at, nonce
                    ) VALUES (
                        %(id)s, %(tenant_id)s, %(name)s, %(email)s, %(token_type)s,
                        %(access_token)s, %(refresh_token)s, %(id_token)s, %(scope)s,
                        %(token_source)s, %(iat)s, %(exp)s, %(nonce)s
                    )

            """
            cursor.execute(insert_query, formatted_token_response)
            conn.commit()
            logger.info("Airflow - database/loadtoDB.py - load_users_tokendata_to_db() - Token data inserted successfully in USERS table")

        except Exception as e:
            logger.error(f"Airflow - database/loadtoDB.py - load_users_tokendata_to_db() - Error inserting token data into the users table = {e}")
            raise e
        finally:
            close_connection(conn, cursor)


# Function to load email data into EMAILS table
def insert_email_data(logger, email_data):
    logger.info("Airflow - database/loadtoDB.py - insert_email_data() - Loading email data into EMAILS table")
    logger.info("Airflow - database/loadtoDB.py - insert_email_data() - Creating database connection")

    conn = create_connection_to_postgresql()

    if conn:
        try:
            cursor = conn.cursor()
            email_insert_query = f"""
                    INSERT INTO emails (
                    id, content_type, body, body_preview, change_key, conversation_id, conversation_index, 
                    created_datetime, created_datetime_timezone, end_datetime, end_datetime_timezone, 
                    has_attachments, importance, inference_classification, is_draft, is_read, 
                    is_all_day, is_out_of_date, meeting_message_type, meeting_request_type, 
                    odata_etag, odata_value, parent_folder_id, received_datetime, recurrence, 
                    reply_to, response_type, sent_datetime, start_datetime, start_datetime_timezone, 
                    subject, type, web_link
                ) VALUES (
                    %(id)s, %(content_type)s, %(body)s, %(body_preview)s, %(change_key)s, %(conversation_id)s, %(conversation_index)s,
                    %(created_datetime)s, %(created_datetime_timezone)s, %(end_datetime)s, %(end_datetime_timezone)s,
                    %(has_attachments)s, %(importance)s, %(inference_classification)s, %(is_draft)s, %(is_read)s,
                    %(is_all_day)s, %(is_out_of_date)s, %(meeting_message_type)s, %(meeting_request_type)s,
                    %(odata_etag)s, %(odata_value)s, %(parent_folder_id)s, %(received_datetime)s, %(recurrence)s,
                    %(reply_to)s, %(response_type)s, %(sent_datetime)s, %(start_datetime)s, %(start_datetime_timezone)s,
                    %(subject)s, %(type)s, %(web_link)s
                )
                """

            cursor.execute(email_insert_query, email_data)
            conn.commit()
            logger.info("Airflow - database/loadtoDB.py - insert_email_data() - Email contents inserted successfully in EMAILS table")

        except Exception as e:
            logger.error(f"Airflow - database/loadtoDB.py - insert_email_data() - Error inserting email contents into the EMAILS table = {e}")
            raise e
        finally:
            close_connection(conn, cursor)


# Function to load sender data
def insert_sender_data(logger, sender_data):
    logger.info("Airflow - database/loadtoDB.py - insert_sender_data() - Loading senders data into SENDERS table")
    logger.info("Airflow - database/loadtoDB.py - insert_sender_data() - Creating database connection")

    conn = create_connection_to_postgresql()

    if conn:
        try:
            cursor = conn.cursor()
            sender_insert_query = f"""
                    INSERT INTO senders (
                        id, email_id, email_address, name
                    ) VALUES (
                        %(id)s, %(email_id)s, %(email_address)s, %(name)s
                    )
                """

            cursor.execute(sender_insert_query, sender_data)
            conn.commit()
            logger.info("Airflow - database/loadtoDB.py - insert_sender_data() - Senders contents inserted successfully in SENDERS table")

        except Exception as e:
            logger.error(f"Airflow - database/loadtoDB.py - insert_sender_data() - Error inserting sender contents into the SENDERS table = {e}")
            raise e
        finally:
            close_connection(conn, cursor)


# Function to load recipient data
def insert_recipient_data(logger, recipients_data):
    logger.info("Airflow - database/loadtoDB.py - insert_recipient_data() - Loading recipients data into RECIPIENTS table")
    logger.info("Airflow - database/loadtoDB.py - insert_recipient_data() - Creating database connection")

    conn = create_connection_to_postgresql()

    if conn:
        try:
            cursor = conn.cursor()
            recipient_insert_query = f"""
                    INSERT INTO recipients (
                        id, email_id, type, email_address, name
                    ) VALUES (
                        %(id)s, %(email_id)s, %(type)s, %(email_address)s, %(name)s
                    )
                """

            for recipient in recipients_data:
                cursor.execute(recipient_insert_query, recipient)
            conn.commit()
            logger.info("Airflow - database/loadtoDB.py - insert_recipient_data() - RECIPIENTS contents inserted successfully in RECIPIENTS table")

        except Exception as e:
            logger.error(f"Airflow - database/loadtoDB.py - insert_recipient_data() - Error inserting RECIPIENTS contents into the RECIPIENTS table = {e}")
            raise e
        finally:
            close_connection(conn, cursor)


# Function to load flags data
def insert_flags_data(logger, flags_data):
    logger.info("Airflow - database/loadtoDB.py - insert_flags_data() - Loading flags data into FLAGS table")
    logger.info("Airflow - database/loadtoDB.py - insert_flags_data() - Creating database connection")

    conn = create_connection_to_postgresql()

    if conn:
        try:
            cursor = conn.cursor()
            flags_insert_query = f"""
                    INSERT INTO flags (
                        email_id, flag_status
                    ) VALUES (
                        %(email_id)s, %(flag_status)s
                    )
                """

            cursor.execute(flags_insert_query, flags_data)
            conn.commit()
            logger.info("Airflow - database/loadtoDB.py - insert_flags_data() - FLAGS contents inserted successfully in FLAGS table")

        except Exception as e:
            logger.error(f"Airflow - database/loadtoDB.py - insert_flags_data() - Error inserting FLAGS contents into the FLAGS table = {e}")
            raise e
        finally:
            close_connection(conn, cursor)


# Function to load emails info
def load_email_info_to_db(logger, formatted_mail_responses):

    logger.info("Airflow - database/loadtoDB.py - load_email_info_to_db() - Loading mail information into the database")

    for email in formatted_mail_responses.get("value", []):

        email_data = {
            "id"                                : email.get("id"),
            "content_type"                      : email.get("body", {}).get("contentType", "html"),
            "body"                              : email.get("body", {}).get("content", ""),
            "body_preview"                      : email.get("bodyPreview", ""),
            "change_key"                        : email.get("changeKey", ""),
            "conversation_id"                   : email.get("conversationId", ""),
            "conversation_index"                : email.get("conversationIndex", ""),
            "created_datetime"                  : email.get("createdDateTime", None) or None,
            "created_datetime_timezone"         : email.get("createdDateTime", None) or None,
            "end_datetime"                      : email.get("endDateTime", {}).get("dateTime", None) or None,
            "end_datetime_timezone"             : email.get("endDateTime", {}).get("timeZone", None) or None,
            "has_attachments"                   : email.get("hasAttachments", False),
            "importance"                        : email.get("importance", ""),
            "inference_classification"          : email.get("inferenceClassification", ""),
            "is_draft"                          : email.get("isDraft", False),
            "is_read"                           : email.get("isRead", False),
            "is_all_day"                        : email.get("isAllDay", False),
            "is_out_of_date"                    : email.get("isOutOfDate", False),
            "meeting_message_type"              : email.get("meetingMessageType", ""),
            "meeting_request_type"              : email.get("meetingRequestType", ""),
            "odata_etag"                        : email.get("@odata.etag", ""),
            "odata_value"                       : email.get("@odata.value", ""),
            "parent_folder_id"                  : email.get("parentFolderId", ""),
            "received_datetime"                 : email.get("receivedDateTime", None) or None,
            "recurrence"                        : json.dumps(email.get("recurrence", {})),
            "reply_to"                          : json.dumps(email.get("replyTo", [])),
            "response_type"                     : email.get("responseType", ""),
            "sent_datetime"                     : email.get("sentDateTime", None) or None,
            "start_datetime"                    : email.get("startDateTime", {}).get("dateTime", None) or None,
            "start_datetime_timezone"           : email.get("startDateTime", {}).get("timeZone", None) or None,
            "subject"                           : email.get("subject", ""),
            "type"                              : email.get("type", ""),
            "web_link"                          : email.get("webLink", "")
        }

        logger.info(f"Airflow - database/loadtoDB.py - load_email_info_to_db() - Loading mail contents to EMAILS table in database")
        insert_email_data(logger, email_data)
        logger.info(f"Airflow - database/loadtoDB.py - load_email_info_to_db() - Email contents uploaded to EMAILS table in database")


        sender_info = email.get("sender", {}).get("emailAddress", {})
        sender_dict = ast.literal_eval(sender_info)
        sender_data = {
            "id": str(uuid.uuid4()),
            "email_id": email.get("id", ""),
            "email_address": sender_dict.get("address", ""),
            "name": sender_dict.get("name", "")
        }
        logger.info(f"Airflow - database/loadtoDB.py - load_email_info_to_db() - Loading sender contents to SENDERS table in database")
        insert_sender_data(logger, sender_data)
        logger.info(f"Airflow - database/loadtoDB.py - load_email_info_to_db() - Sender contents uploaded to SENDERS table in database")


        recipients_data = []
        for recipient_type, recipients_key in [("to", "toRecipients"), ("cc", "ccRecipients"), ("bcc", "bccRecipients")]:
            for recipient in email.get(recipients_key, []):
                recipient_info = recipient.get("emailAddress", "")
                recipient_dict = ast.literal_eval(recipient_info)
                recipients_data.append({
                    "id": str(uuid.uuid4()),
                    "email_id": email.get("id", ""),
                    "type": recipient_type,
                    "email_address": recipient_dict.get('address', ""),
                    "name": recipient_dict.get('name', "")
                })
        logger.info(f"Airflow - database/loadtoDB.py - load_email_info_to_db() - Loading recipient contents to RECIPIENTS table in database")
        insert_recipient_data(logger, recipients_data)
        logger.info(f"Airflow - database/loadtoDB.py - load_email_info_to_db() - recipient contents uploaded to RECIPIENTS table in database")


        flag_data = {
            "email_id": email.get("id", ""),
            "flag_status": email.get("flag", {}).get("flagStatus","")
        }
        logger.info(f"Airflow - database/loadtoDB.py - load_email_info_to_db() - Loading flags contents to FLAGS table in database")
        insert_flags_data(logger, flag_data)
        logger.info(f"Airflow - database/loadtoDB.py - load_email_info_to_db() - flags contents uploaded to FLAGS table in database")
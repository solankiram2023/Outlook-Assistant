import ast
import uuid
import json

from database.connectDB import create_connection_to_postgresql, close_connection
from services.vectors import create_embeddings_and_index

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
                ON CONFLICT (id) 
                DO UPDATE SET
                    tenant_id = EXCLUDED.tenant_id,
                    name = EXCLUDED.name,
                    email = EXCLUDED.email,
                    token_type = EXCLUDED.token_type,
                    access_token = EXCLUDED.access_token,
                    refresh_token = EXCLUDED.refresh_token,
                    id_token = EXCLUDED.id_token,
                    scope = EXCLUDED.scope,
                    token_source = EXCLUDED.token_source,
                    issued_at = EXCLUDED.issued_at,
                    expires_at = EXCLUDED.expires_at,
                    nonce = EXCLUDED.nonce
            """
            cursor.execute(insert_query, formatted_token_response)
            conn.commit()
            logger.info("Airflow - database/loadtoDB.py - load_users_tokendata_to_db() - Token data inserted successfully in USERS table")

        except Exception as e:
            logger.error(f"Airflow - database/loadtoDB.py - load_users_tokendata_to_db() - Error inserting token data into the users table = {e}")
            raise e
        finally:
            close_connection(conn, cursor)


# Fuction to load email link data into EMAIL_LINKS table
def insert_or_update_email_links(logger, email_link_data):
    logger.info("Airflow - database/loadtoDB.py - insert_or_update_email_links() - Inserting or updating email links data in EMAIL_LINKS table")
    logger.info("Airflow - database/loadtoDB.py - insert_or_update_email_links() - Creating database connection")

    conn = create_connection_to_postgresql()

    if conn:
        try:
            cursor = conn.cursor()
            email_links_query = f"""
                INSERT INTO email_links (
                    id, email, current_link, next_link, is_current_link_processed
                ) VALUES (
                    %(id)s, %(email)s, %(current_link)s, %(next_link)s, %(is_current_link_processed)s
                )
                ON CONFLICT (id) 
                DO UPDATE SET
                    current_link = EXCLUDED.current_link,
                    next_link = EXCLUDED.next_link,
                    is_current_link_processed = EXCLUDED.is_current_link_processed,
                    updated_at = CURRENT_TIMESTAMP
            """

            cursor.execute(email_links_query, email_link_data)
            conn.commit()
            logger.info("Airflow - database/loadtoDB.py - insert_or_update_email_links() - Email links data inserted or updated successfully in EMAIL_LINKS table")

        except Exception as e:
            logger.error(f"Airflow - database/loadtoDB.py - insert_or_update_email_links() - Error inserting or updating email links data: {e}")
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
                ON CONFLICT (id)
                DO UPDATE SET
                    content_type = EXCLUDED.content_type,
                    body = EXCLUDED.body,
                    body_preview = EXCLUDED.body_preview,
                    change_key = EXCLUDED.change_key,
                    conversation_id = EXCLUDED.conversation_id,
                    conversation_index = EXCLUDED.conversation_index,
                    created_datetime = EXCLUDED.created_datetime,
                    created_datetime_timezone = EXCLUDED.created_datetime_timezone,
                    end_datetime = EXCLUDED.end_datetime,
                    end_datetime_timezone = EXCLUDED.end_datetime_timezone,
                    has_attachments = EXCLUDED.has_attachments,
                    importance = EXCLUDED.importance,
                    inference_classification = EXCLUDED.inference_classification,
                    is_draft = EXCLUDED.is_draft,
                    is_read = EXCLUDED.is_read,
                    is_all_day = EXCLUDED.is_all_day,
                    is_out_of_date = EXCLUDED.is_out_of_date,
                    meeting_message_type = EXCLUDED.meeting_message_type,
                    meeting_request_type = EXCLUDED.meeting_request_type,
                    odata_etag = EXCLUDED.odata_etag,
                    odata_value = EXCLUDED.odata_value,
                    parent_folder_id = EXCLUDED.parent_folder_id,
                    received_datetime = EXCLUDED.received_datetime,
                    recurrence = EXCLUDED.recurrence,
                    reply_to = EXCLUDED.reply_to,
                    response_type = EXCLUDED.response_type,
                    sent_datetime = EXCLUDED.sent_datetime,
                    start_datetime = EXCLUDED.start_datetime,
                    start_datetime_timezone = EXCLUDED.start_datetime_timezone,
                    subject = EXCLUDED.subject,
                    type = EXCLUDED.type,
                    web_link = EXCLUDED.web_link
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
                    ON CONFLICT (id) 
                    DO UPDATE SET
                        email_id = EXCLUDED.email_id,
                        email_address = EXCLUDED.email_address,
                        name = EXCLUDED.name
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
                    ON CONFLICT (id) 
                    DO UPDATE SET
                        email_id = EXCLUDED.email_id,
                        type = EXCLUDED.type,
                        email_address = EXCLUDED.email_address,
                        name = EXCLUDED.name
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
                    ON CONFLICT (email_id) 
                    DO UPDATE SET
                        flag_status = EXCLUDED.flag_status
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
def load_email_info_to_db(logger, formatted_mail_responses, user_email):
    logger.info("Airflow - database/loadtoDB.py - load_email_info_to_db() - Loading mail information into the database")

    for email in formatted_mail_responses:
        # Email data
        email_data = {
            "id"                        : email.get("id"),
            "content_type"              : email.get("body", None).get("contentType", "html"),
            "body"                      : email.get("body", None).get("content", ""),
            "body_preview"              : email.get("bodyPreview", None),
            "change_key"                : email.get("changeKey", None),
            "conversation_id"           : email.get("conversationId", None),
            "conversation_index"        : email.get("conversationIndex", None),
            "created_datetime"          : email.get("createdDateTime", None) or None,
            "created_datetime_timezone" : email.get("createdDateTime", None) or None,
            "end_datetime"              : email.get("endDateTime", {}).get("dateTime", None) or None,
            "end_datetime_timezone"     : email.get("endDateTime", {}).get("timeZone", None) or None,
            "has_attachments"           : email.get("hasAttachments", False),
            "importance"                : email.get("importance", None),
            "inference_classification"  : email.get("inferenceClassification", None),
            "is_draft"                  : email.get("isDraft", False),
            "is_read"                   : email.get("isRead", False),
            "is_all_day"                : email.get("isAllDay", False),
            "is_out_of_date"            : email.get("isOutOfDate", False),
            "meeting_message_type"      : email.get("meetingMessageType", None),
            "meeting_request_type"      : email.get("meetingRequestType", None),
            "odata_etag"                : email.get("@odata.etag", None),
            "odata_value"               : email.get("@odata.value", None),
            "parent_folder_id"          : email.get("parentFolderId", None),
            "received_datetime"         : email.get("receivedDateTime", None) or None,
            "recurrence"                : json.dumps(email.get("recurrence")) if email.get("recurrence", None) else None,
            "reply_to"                  : json.dumps(email.get("replyTo")) if email.get("replyTo", None) else None,
            "response_type"             : email.get("responseType", None),
            "sent_datetime"             : email.get("sentDateTime", None) or None,
            "start_datetime"            : email.get("startDateTime", {}).get("dateTime", None) or None,
            "start_datetime_timezone"   : email.get("startDateTime", {}).get("timeZone", None) or None,
            "subject"                   : email.get("subject", None),
            "type"                      : email.get("type", None),
            "web_link"                  : email.get("webLink", None)
        }

        # Sender data
        sender_info = email.get("sender", {}).get("emailAddress", {})
        sender_dict = ast.literal_eval(sender_info)
        sender_data = {
            "id": str(uuid.uuid4()),
            "email_id": email.get("id", ""),
            "email_address": sender_dict.get("address", ""),
            "name": sender_dict.get("name", "")
        }

        # Recipient data
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

        # Email flags data
        flag_data = {
            "email_id": email.get("id", ""),
            "flag_status": email.get("flag", {}).get("flagStatus","")
        }

        # Insert email data into Postgres
        logger.info(f"Airflow - database/loadtoDB.py - load_email_info_to_db() - Loading mail contents to EMAILS table in database")
        insert_email_data(logger, email_data)
        logger.info(f"Airflow - database/loadtoDB.py - load_email_info_to_db() - Email contents uploaded to EMAILS table in database")

        # Insert sender data into Postgres
        logger.info(f"Airflow - database/loadtoDB.py - load_email_info_to_db() - Loading sender contents to SENDERS table in database")
        insert_sender_data(logger, sender_data)
        logger.info(f"Airflow - database/loadtoDB.py - load_email_info_to_db() - Sender contents uploaded to SENDERS table in database")

        # Insert recipient data into Postgres
        logger.info(f"Airflow - database/loadtoDB.py - load_email_info_to_db() - Loading recipient contents to RECIPIENTS table in database")
        insert_recipient_data(logger, recipients_data)
        logger.info(f"Airflow - database/loadtoDB.py - load_email_info_to_db() - recipient contents uploaded to RECIPIENTS table in database")

        # Insert flag data into Postgres        
        logger.info(f"Airflow - database/loadtoDB.py - load_email_info_to_db() - Loading flags contents to FLAGS table in database")
        insert_flags_data(logger, flag_data)
        logger.info(f"Airflow - database/loadtoDB.py - load_email_info_to_db() - flags contents uploaded to FLAGS table in database")

        # Finally, index the email contents in Milvus
        data_to_index = {
            "subject"           : email_data["subject"],
            "body"              : email_data["body"],
            "reply_to"          : email_data["reply_to"],
            "created_datetime"  : email_data["created_datetime"],
            "received_datetime" : email_data["received_datetime"],
            "sent_datetime"     : email_data["sent_datetime"],
        }

        metadata = {
            "id"                    : email_data["id"],
            "user_email"            : user_email,
            "conversation_id"       : email_data["conversation_id"],
            "conversation_index"    : email_data["conversation_index"],
            "message_type"          : "email"
        }

        create_embeddings_and_index(data_to_index=data_to_index, metadata=metadata)
from database.connectDB import create_connection_to_postgresql, close_connection

# Function to create tables in PostgreSQL database
def create_tables_in_db(logger):
    logger.info("Airflow - POSTGRESQL - database/setupTables.py - create_tables_in_db() - Dropping the existing tables and Creating tables in PostgreSQL database")

    queries = {
        "drop_tables": {
                "drop_users_table"                  : "DROP TABLE IF EXISTS users;",
                "drop_emails_table"                 : "DROP TABLE IF EXISTS emails CASCADE;",
                "drop_recipients_table"             : "DROP TABLE IF EXISTS recipients CASCADE;",
                "drop_senders_table"                : "DROP TABLE IF EXISTS senders CASCADE;",
                "drop_attachments_table"            : "DROP TABLE IF EXISTS attachments CASCADE;",
                "drop_flags_table"                  : "DROP TABLE IF EXISTS flags CASCADE;",
                "drop_categories_table"             : "DROP TABLE IF EXISTS categories CASCADE;",
            },
        "create_tables": {
                "create_users_table": """
                    CREATE TABLE IF NOT EXISTS users (
                    id VARCHAR(255) PRIMARY KEY,
                    tenant_id VARCHAR(255),
                    name VARCHAR(255),
                    email VARCHAR(255) UNIQUE,
                    token_type VARCHAR(50),
                    access_token TEXT,
                    refresh_token TEXT,
                    id_token TEXT,
                    scope TEXT,
                    token_source VARCHAR(50),
                    issued_at TIMESTAMP,
                    expires_at TIMESTAMP,
                    nonce VARCHAR(255)
            );
                """,
                "create_emails_table": """
                CREATE TABLE IF NOT EXISTS emails (
                    body TEXT,
                    body_preview TEXT,
                    change_key VARCHAR(255),
                    content_type VARCHAR(255) DEFAULT 'html',
                    conversation_id VARCHAR(255),
                    conversation_index TEXT,
                    created_datetime TIMESTAMPTZ,
                    created_datetime_timezone VARCHAR(50),
                    end_datetime TIMESTAMPTZ,
                    end_datetime_timezone VARCHAR(50),
                    has_attachments BOOLEAN DEFAULT FALSE,
                    id VARCHAR(255) PRIMARY KEY,
                    importance VARCHAR(50),
                    inference_classification VARCHAR(50),
                    is_draft BOOLEAN DEFAULT FALSE,
                    is_read BOOLEAN,
                    is_all_day BOOLEAN,
                    is_out_of_date BOOLEAN,
                    meeting_message_type VARCHAR(255),
                    meeting_request_type VARCHAR(255),
                    odata_etag TEXT,
                    odata_value TEXT,
                    parent_folder_id VARCHAR(255),
                    received_datetime TIMESTAMPTZ,
                    recurrence TEXT,
                    reply_to TEXT,
                    response_type VARCHAR(50),
                    sent_datetime TIMESTAMPTZ,
                    start_datetime TIMESTAMPTZ,
                    start_datetime_timezone VARCHAR(50),
                    subject TEXT,
                    type VARCHAR(50),
                    web_link TEXT
                );
                """,
                "create_recipients_table": """
                CREATE TABLE IF NOT EXISTS recipients (
                    id VARCHAR(255) PRIMARY KEY,
                    email_id VARCHAR(255) REFERENCES emails(id),
                    type VARCHAR(50),
                    email_address VARCHAR(255),
                    name VARCHAR(255)
                );
                """,
                "create_senders_table": """
                CREATE TABLE IF NOT EXISTS senders (
                    id VARCHAR(255) PRIMARY KEY,
                    email_id VARCHAR(255) REFERENCES emails(id),
                    email_address VARCHAR(255),
                    name VARCHAR(255)
                );
                """,
                "create_attachments_table": """
                CREATE TABLE IF NOT EXISTS attachments (
                    id VARCHAR(255) PRIMARY KEY,
                    email_id VARCHAR(255) REFERENCES emails(id),
                    name TEXT,
                    content_type TEXT,
                    size BIGINT,
                    bucket_url TEXT
                );
                """,
                "create_flags_table": """
                CREATE TABLE IF NOT EXISTS flags (
                    email_id VARCHAR(255) PRIMARY KEY REFERENCES emails(id),
                    flag_status VARCHAR(50)
                );
                """,
                "create_categories_table": """
                    CREATE TABLE IF NOT EXISTS categories (
                        id VARCHAR(255) PRIMARY KEY,
                        email_id VARCHAR(255) REFERENCES emails(id),
                        category TEXT,
                        user_defined_category TEXT
                    );
                """,
            },
    }

    conn = create_connection_to_postgresql()

    if conn:
        try:
            cursor = conn.cursor()
            logger.info("Airflow - POSTGRESQL - database/setupTables.py - create_tables_in_db() - DB Connection & cursor created successfully")

            # Execute drop table queries
            logger.info("Airflow - POSTGRESQL - database/setupTables.py - create_tables_in_db() - Dropping existing tables")
            for table_name, drop_query in queries["drop_tables"].items():
                try:
                    logger.info(f"Airflow - POSTGRESQL - database/setupTables.py - create_tables_in_db() - Executing drop query for table: {table_name}")
                    cursor.execute(drop_query)
                    logger.info(f"Airflow - POSTGRESQL - database/setupTables.py - create_tables_in_db() - Table '{table_name}' dropped successfully.")
                except Exception as e:
                    logger.error(f"Airflow - POSTGRESQL - database/setupTables.py - create_tables_in_db() - Error dropping table '{table_name}': {e}")

            # Execute create table queries
            logger.info("Airflow - POSTGRESQL - database/setupTables.py - create_tables_in_db() - Creating new tables")
            for table_name, create_query in queries["create_tables"].items():
                try:
                    logger.info(f"Airflow - POSTGRESQL - database/setupTables.py - create_tables_in_db() - Executing create query for table: {table_name}")
                    cursor.execute(create_query)
                    logger.info(f"Airflow - POSTGRESQL - database/setupTables.py - create_tables_in_db() - Table '{table_name}' created successfully.")
                except Exception as e:
                    logger.error(f"Airflow - POSTGRESQL - database/setupTables.py - create_tables_in_db() - Error creating table '{table_name}': {e}")
            
            conn.commit()
            logger.info(f"Airflow - POSTGRESQL - database/setupTables.py - create_tables_in_db() - All tables dropped and created successfully")

        except Exception as e:
            logger.error(f"Airflow - POSTGRESQL - database/setupTables.py - create_tables_in_db() - Error executing table queries: {e}")

        finally:
            close_connection(conn, cursor)
            logger.info(f"Airflow - POSTGRESQL - database/setupTables.py - create_tables_in_db() - Connection to the DB closed")    

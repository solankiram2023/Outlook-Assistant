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
                "drop_email_links_table"            : "DROP TABLE IF EXISTS email_links CASCADE;",
                "drop_queued_jobs_table"            : "DROP TABLE IF EXISTS queued_jobs CASCADE;",
                "drop_email_folders_table"          : "DROP TABLE IF EXISTS email_folders CASCADE"
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
                    body TEXT DEFAULT NULL,
                    body_preview TEXT DEFAULT NULL,
                    change_key VARCHAR(255) DEFAULT NULL,
                    content_type VARCHAR(255) DEFAULT 'html',
                    conversation_id VARCHAR(255) DEFAULT NULL,
                    conversation_index TEXT DEFAULT NULL,
                    created_datetime TIMESTAMPTZ DEFAULT NULL,
                    created_datetime_timezone VARCHAR(50) DEFAULT NULL,
                    end_datetime TIMESTAMPTZ DEFAULT NULL,
                    end_datetime_timezone VARCHAR(50) DEFAULT NULL,
                    has_attachments BOOLEAN DEFAULT FALSE,
                    id VARCHAR(255) PRIMARY KEY,
                    importance VARCHAR(50) DEFAULT NULL,
                    inference_classification VARCHAR(50) DEFAULT NULL,
                    is_draft BOOLEAN DEFAULT FALSE,
                    is_read BOOLEAN DEFAULT NULL,
                    is_all_day BOOLEAN DEFAULT NULL,
                    is_out_of_date BOOLEAN DEFAULT NULL,
                    meeting_message_type VARCHAR(255) DEFAULT NULL,
                    meeting_request_type VARCHAR(255) DEFAULT NULL,
                    odata_etag TEXT DEFAULT NULL,
                    odata_value TEXT DEFAULT NULL,
                    parent_folder_id VARCHAR(255) DEFAULT NULL,
                    received_datetime TIMESTAMPTZ DEFAULT NULL,
                    recurrence TEXT DEFAULT NULL,
                    reply_to TEXT DEFAULT NULL,
                    response_type VARCHAR(50) DEFAULT NULL,
                    sent_datetime TIMESTAMPTZ DEFAULT NULL,
                    start_datetime TIMESTAMPTZ DEFAULT NULL,
                    start_datetime_timezone VARCHAR(50) DEFAULT NULL,
                    subject TEXT DEFAULT NULL,
                    type VARCHAR(50) DEFAULT NULL,
                    web_link TEXT DEFAULT NULL,
                    vector_indexed BOOLEAN DEFAULT FALSE
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
                "create_queued_jobs_table": """
                    CREATE TABLE queued_jobs (
                        id SERIAL PRIMARY KEY, 
                        email VARCHAR(255), 
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
                        status VARCHAR(50),
                        updated_at TIMESTAMP DEFAULT '1970-01-01 00:00:00'
                    );
                """,
                "create_email_links_table": """
                    CREATE TABLE IF NOT EXISTS email_links (
                        id VARCHAR(255) PRIMARY KEY,
                        email VARCHAR(255) UNIQUE,
                        current_link TEXT DEFAULT NULL,
                        next_link TEXT DEFAULT NULL,
                        is_current_link_processed BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """,
                "create_email_folders_table": """
                    CREATE TABLE email_folders (
                        id VARCHAR(255) PRIMARY KEY,
                        display_name VARCHAR(255) NOT NULL,
                        parent_folder_id VARCHAR(255),
                        child_folder_count INT DEFAULT 0,
                        unread_item_count INT DEFAULT 0,
                        total_item_count INT DEFAULT 0,
                        size_in_bytes BIGINT DEFAULT 0,
                        is_hidden BOOLEAN DEFAULT FALSE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """

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
                    conn.commit()
                    logger.info(f"Airflow - POSTGRESQL - database/setupTables.py - create_tables_in_db() - Table '{table_name}' dropped successfully.")
                except Exception as e:
                    logger.error(f"Airflow - POSTGRESQL - database/setupTables.py - create_tables_in_db() - Error dropping table '{table_name}': {e}")
                    conn.rollback()

            # Execute create table queries
            logger.info("Airflow - POSTGRESQL - database/setupTables.py - create_tables_in_db() - Creating new tables")
            for table_name, create_query in queries["create_tables"].items():
                try:
                    logger.info(f"Airflow - POSTGRESQL - database/setupTables.py - create_tables_in_db() - Executing create query for table: {table_name}")
                    cursor.execute(create_query)
                    conn.commit()
                    logger.info(f"Airflow - POSTGRESQL - database/setupTables.py - create_tables_in_db() - Table '{table_name}' created successfully.")
                except Exception as e:
                    logger.error(f"Airflow - POSTGRESQL - database/setupTables.py - create_tables_in_db() - Error creating table '{table_name}': {e}")
                    conn.rollback()
            
            conn.commit()
            logger.info(f"Airflow - POSTGRESQL - database/setupTables.py - create_tables_in_db() - All tables dropped and created successfully")

        except Exception as e:
            logger.error(f"Airflow - POSTGRESQL - database/setupTables.py - create_tables_in_db() - Error executing table queries: {e}")

        finally:
            close_connection(conn, cursor)
            logger.info(f"Airflow - POSTGRESQL - database/setupTables.py - create_tables_in_db() - Connection to the DB closed")    

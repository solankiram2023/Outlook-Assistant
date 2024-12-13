import os
import json
import boto3
import openai
import logging
import tiktoken
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Tuple, Optional
# from database.dbconnect import create_connection_to_postgresql, close_connection
from database.connection import open_connection, close_connection

from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain.schema.runnable import RunnableSequence
from langchain.tools import tool
from langchain_core.messages import ToolMessage, AIMessage

from utils.logs import start_logger
from agents.state import AgentState

from agents.summary_attachments import (
    parse_images, parse_pdf_files, parse_excel_files, 
    parse_word_file, parse_txt_files, parse_csv_files
)

logging.basicConfig(
    level    = logging.INFO,
    format   = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers = [
        logging.FileHandler('thread_summarization.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize OpenAI with API key from .env
openai.api_key = os.getenv('OPENAI_API_KEY')

llm = ChatOpenAI(
    model       = "gpt-4o",
    temperature = 0.7,
    api_key     = os.getenv("OPENAI_API_KEY")
)

# Logging
logger = start_logger()


class ThreadAnalyzer:
    """ Analyze email threads and attempt to generate summary """
    
    def __init__(self):
        """Initialize ThreadAnalyzer with S3 client."""
        
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id     = os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name           = os.getenv('AWS_REGION')
        )
        logger.info("Initialized ThreadAnalyzer with S3 client")
        

        # Initialize tiktoken encoder for GPT-4
        self.encoding = tiktoken.encoding_for_model("gpt-4")
        self.MAX_TOKENS = 100000
        logger.info("Initialized ThreadAnalyzer with S3 client and tiktoken encoder")

    def process_attachment_content(self, attachment: Dict) -> str:
        """Extract and process content from attachment based on its type."""
        
        try:
            if not attachment.get('bucket_url'):
                return None

            # Parse S3 URL
            bucket_url = attachment['bucket_url']
            bucket_name = bucket_url.split('/')[2]
            key = '/'.join(bucket_url.split('/')[3:])

            # Download file from S3 to temp location
            local_path = f"tmp/{attachment['name']}"
            os.makedirs("tmp", exist_ok=True)
            
            self.s3_client.download_file(bucket_name, key, local_path)

            try:
                content_type = attachment.get('content_type', '').lower()
                content = None

                if any(img_type in content_type for img_type in ['image', 'jpeg', 'png', 'gif', 'bmp']):
                    content = parse_images(logger, local_path)
                elif 'pdf' in content_type:
                    content = parse_pdf_files(logger, local_path)
                elif 'spreadsheet' in content_type or 'excel' in content_type:
                    content = parse_excel_files(logger, local_path)
                elif 'document' in content_type or 'word' in content_type:
                    content = parse_word_file(logger, local_path)
                elif 'text' in content_type or 'csv' in content_type:
                    if local_path.endswith('.csv'):
                        content = parse_csv_files(logger, local_path)
                    else:
                        content = parse_txt_files(logger, local_path)

                return content

            finally:
                # Clean up temporary file
                if os.path.exists(local_path):
                    os.remove(local_path)

        except Exception as e:
            logger.error(f"Error processing attachment {attachment.get('name')}: {str(e)}")
            return None


    def get_conversation_ids(self) -> List[str]:
        """Fetch all unique conversation IDs from the database."""
        
        logger.info("Fetching unique conversation IDs")
        conn = open_connection()
        conversation_ids = []
        
        try:
            with conn.cursor() as cursor:
                query = """
                    SELECT DISTINCT conversation_id 
                    FROM emails 
                    WHERE conversation_id IS NOT NULL
                    AND conversation_id != ''
                    GROUP BY conversation_id
                    HAVING COUNT(*) > 1;  
                """
                cursor.execute(query)
                conversation_ids = [row[0] for row in cursor.fetchall()]
                logger.info(f"Found {len(conversation_ids)} conversation threads")
        
        finally:
            close_connection(conn)
            return conversation_ids

    def get_thread_emails(self, conversation_id: str) -> List[Dict]:
        """Fetch all emails in a thread ordered by sent datetime."""
        
        logger.info(f"Fetching emails for conversation ID: {conversation_id}")
        conn = open_connection()

        thread_emails = None
        
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
                    WITH thread_emails AS (
                        SELECT 
                            e.id,
                            e.subject,
                            e.body,
                            e.body_preview,
                            e.sent_datetime,
                            e.received_datetime,
                            e.importance,
                            e.has_attachments,
                            e.conversation_id,
                            json_agg(
                                DISTINCT jsonb_build_object(
                                    'sender_email', s.email_address,
                                    'sender_name', s.name
                                )
                            ) AS senders,
                            json_agg(
                                DISTINCT jsonb_build_object(
                                    'recipient_email', r.email_address,
                                    'recipient_name', r.name,
                                    'type', r.type
                                )
                            ) AS recipients,
                            CASE 
                                WHEN e.has_attachments THEN 
                                    json_agg(
                                        DISTINCT jsonb_build_object(
                                            'name', a.name,
                                            'content_type', a.content_type,
                                            'size', a.size,
                                            'bucket_url', a.bucket_url
                                        )
                                    ) FILTER (WHERE a.id IS NOT NULL)
                                ELSE '[]'::json
                            END AS attachments
                        FROM 
                            emails e
                            LEFT JOIN senders s ON e.id = s.email_id
                            LEFT JOIN recipients r ON e.id = r.email_id
                            LEFT JOIN attachments a ON e.id = a.email_id
                        WHERE 
                            e.conversation_id = %s
                        GROUP BY 
                            e.id
                    )
                    SELECT *
                    FROM thread_emails
                    ORDER BY sent_datetime ASC NULLS LAST;
                """
                
                cursor.execute(query, (conversation_id,))
                thread_emails = cursor.fetchall()
                logger.info(f"Found {len(thread_emails)} emails in thread")
        
        except Exception as e:
            logger.error(f"Error fetching thread emails: {str(e)}")
            raise
        
        finally:
            close_connection(conn)
            return thread_emails

    def _format_attachment_info(self, attachment: Dict) -> str:
        """Format attachment information for summary."""
        size_mb = float(attachment['size']) / (1024 * 1024) if attachment.get('size') else 0
        return f"{attachment['name']} ({attachment['content_type']}, {size_mb:.2f}MB)"

    def count_tokens(self, text: str) -> int:
        """Count the number of tokens in a text string."""
        return len(self.encoding.encode(text))


    def truncate_to_token_limit(self, text: str, max_tokens: int) -> str:
        """Truncate text to stay within token limit while maintaining coherence."""
        tokens = self.encoding.encode(text)
        if len(tokens) <= max_tokens:
            return text
        
        # Decode only the tokens we want to keep
        truncated_tokens = tokens[:max_tokens]
        return self.encoding.decode(truncated_tokens)


    def prepare_thread_content(self, thread_emails: List[Dict], attachment_contents: List[Dict]) -> Tuple[str, str]:
        """Prepare and truncate thread content and attachment content within token limits."""
        
        # Reserve tokens for the prompt template and response
        RESERVED_TOKENS = 5000
        AVAILABLE_TOKENS = self.MAX_TOKENS - RESERVED_TOKENS
        
        # Allocate tokens between thread content and attachments (70-30 split)
        THREAD_TOKEN_LIMIT = int(AVAILABLE_TOKENS * 0.7)
        ATTACHMENT_TOKEN_LIMIT = int(AVAILABLE_TOKENS * 0.3)

        thread_context = []
        for email in thread_emails:
            sender = email['senders'][0] if email['senders'] else {'sender_name': 'Unknown', 'sender_email': 'unknown'}
            
            email_content = f"""
            Timestamp: {email['sent_datetime']}
            From: {sender['sender_name']} <{sender['sender_email']}>
            To: {', '.join(f"{r['recipient_name']} <{r['recipient_email']}>" for r in email['recipients'])}
            Subject: {email['subject']}
            Importance: {email['importance']}

            Content:
            {email['body_preview'] if email['body_preview'] else email['body']}

            ---
            """
            thread_context.append(email_content)

        # Join and truncate thread content
        full_thread_content = "\n".join(thread_context)
        truncated_thread_content = self.truncate_to_token_limit(full_thread_content, THREAD_TOKEN_LIMIT)

        # Prepare and truncate attachment content
        attachment_summary = "\n\n".join([
            f"Attachment: {att['name']}\nContent:\n{att['content']}"
            for att in attachment_contents
        ]) if attachment_contents else "No attachment content available"
        
        truncated_attachment_content = self.truncate_to_token_limit(attachment_summary, ATTACHMENT_TOKEN_LIMIT)

        return truncated_thread_content, truncated_attachment_content


    def summarize_thread(self, thread_emails: List[Dict]) -> Dict:
        """Summarize thread with token counting and limiting."""
        try:
            attachment_contents = []

            # Process attachments
            for email in thread_emails:
                if email['has_attachments'] and email['attachments']:
                    for attachment in email['attachments']:
                        content = self.process_attachment_content(attachment)
                        if content:
                            attachment_contents.append({
                                'name': attachment['name'],
                                'content': content
                            })

            # Prepare and truncate content within token limits
            thread_content, attachment_content = self.prepare_thread_content(
                thread_emails, 
                attachment_contents
            )

            # Calculate total tokens used
            total_tokens = (
                self.count_tokens(thread_content) +
                self.count_tokens(attachment_content)
            )

            logger.info(f"Total tokens used: {total_tokens}")

            # Define the LangChain prompt
            prompt_template = """
            Analyze the following email thread and its attachments:
            
            Thread Overview:
            - Total Emails: {email_count}
            - Time Span: {time_span}
            - Thread Subject: {thread_subject}

            Email Thread:
            {thread_content}

            Attachment Contents:
            {attachment_content}

            Please provide:
            1. Thread Summary: A concise overview of the entire conversation.
            2. Key Points: Main topics discussed and their progression.
            3. Decisions & Action Items: Any decisions made or actions required.
            4. Attachment Analysis: Summary of the content found in attachments.
            5. Timeline: Key developments in chronological order including attachment content.
            """
            prompt = PromptTemplate(
                input_variables=["email_count", "time_span", "thread_subject", "thread_content", "attachment_content"],
                template=prompt_template
            )

            chain = RunnableSequence(prompt | llm)

            # Execute the LangChain chain
            response = chain.invoke({
                "email_count": len(thread_emails),
                "time_span": f"{thread_emails[0]['sent_datetime']} to {thread_emails[-1]['sent_datetime']}",
                "thread_subject": thread_emails[0]['subject'],
                "thread_content": thread_content,
                "attachment_content": attachment_content
            })

            # Extract the summarized content
            summary_content = response.content.strip()

            # Collect participants data
            participants = self._get_unique_participants(thread_emails)

            # Return the summary with token information
            return {
                'conversation_id': thread_emails[0]['conversation_id'],
                'summary': summary_content
            }

        except Exception as e:
            logger.error(f"Error generating thread summary: {str(e)}")
            raise


    def _get_unique_participants(self, thread_emails: List[Dict]) -> Dict[str, Dict]:
        """Extract unique participants and their roles in the thread."""
        participants = {}
        for email in thread_emails:
            # Add sender
            sender = email['senders'][0] if email['senders'] else {'sender_name': 'Unknown', 'sender_email': 'unknown'}
            if sender['sender_email'] not in participants:
                participants[sender['sender_email']] = {
                    'name': sender['sender_name'],
                    'sent_count': 0,
                    'received_count': 0
                }
            participants[sender['sender_email']]['sent_count'] += 1

            # Add recipients
            for recipient in email['recipients']:
                if recipient['recipient_email'] not in participants:
                    participants[recipient['recipient_email']] = {
                        'name': recipient['recipient_name'],
                        'sent_count': 0,
                        'received_count': 0
                    }
                participants[recipient['recipient_email']]['received_count'] += 1

        return participants


def fetch_emailId_from_conversationId(email_id: str):
    """ Given an email ID, fetch its respective conversation ID from the database """

    conversation_id = None

    if not email_id:
        logger.error(f"Unable to fetch conversation_id from database because email_id is None or empty: {email_id}")

        return conversation_id
    
    conn = open_connection()
    
    if not conn:
        logger.error(f"Unable to fetch conversation_id from database because database connection failed")
        return conversation_id
    
    fetch_conversation_id_query = """
        SELECT conversation_id
        FROM emails
        WHERE id = %s
        LIMIT 1;
    """

    try:
        with conn.cursor() as cursor:
            cursor.execute(fetch_conversation_id_query, (email_id,))
            result = cursor.fetchone()

            if result:
                conversation_id = result[0]
                logger.info(f"Fetched coversation_id {conversation_id} for email_id {email_id}")

            else:
                logger.error(f"No conversation_id found for email_id {email_id}")

    except Exception as exception:
        logger.error(f"Exception occurred in fetch_emailId_from_conversationId() : {exception}")

    finally:
        close_connection(conn)
        return conversation_id


def generate_filename(conversation_id: str) -> str:
    """Generate a valid JSON filename based on conversation_id."""
    
    return f"{conversation_id}.json"

def summarize_single_thread(conversation_id: str, output_dir: str = "./summaries") -> Dict:
    """ Process and summarize a single email thread based on conversation_id """
    
    try:
        analyzer = ThreadAnalyzer()
        
        # Ensure output directory exists
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Fetch emails for this thread
        thread_emails = analyzer.get_thread_emails(conversation_id)

        if not thread_emails:
            logger.warning(f"No emails found for thread {conversation_id}")
            return {
                'status': 'error',
                'message': f"No emails found for thread {conversation_id}",
                'conversation_id': conversation_id
            }

        # Generate filename based on conversation_id
        filename = generate_filename(conversation_id)
        output_file = os.path.join(output_dir, filename)

        # Generate summary
        summary = analyzer.summarize_thread(thread_emails)

        # Add subject 
        subject = thread_emails[0]['subject']
        summary['subject'] = subject

        # Save summary to JSON file
        with open(output_file, "w") as json_file:
            json.dump(summary, json_file, indent=4)
            logger.info(f"Saved summary for conversation_id {conversation_id} to {output_file}")

        return {
            'status': 'success',
            **summary
        }

    except Exception as e:
        logger.error(f"Error processing thread {conversation_id}: {str(e)}")
        return {
            'status': 'error',
            'message': str(e),
            'conversation_id': conversation_id
        }

def load_thread_summary(conversation_id: str, output_dir: str = "./summaries") -> Optional[Dict]:
    """ Load an existing thread summary from JSON file if it exists """
    
    try:
        filename = generate_filename(conversation_id)
        filepath = os.path.join(output_dir, filename)
        
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                return json.load(f)
        return None
        
    except Exception as e:
        logger.error(f"Error loading summary for {conversation_id}: {str(e)}")
        return None

def get_or_create_thread_summary(conversation_id: str, output_dir: str = "./summaries", force_refresh: bool = False) -> Dict:
    """ Get existing summary or create new one for a conversation thread """
    
    try:
        if not force_refresh:
            # Try to load existing summary
            existing_summary = load_thread_summary(conversation_id, output_dir)
            if existing_summary:
                logger.info(f"Found existing summary for conversation {conversation_id}")
                return {
                    'status': 'success',
                    **existing_summary
                }
        
        # Generate new summary
        logger.info(f"Generating new summary for conversation {conversation_id}")
        return summarize_single_thread(conversation_id, output_dir)

    except Exception as e:
        logger.error(f"Error in get_or_create_thread_summary: {str(e)}")
        return {
            'status': 'error',
            'message': str(e),
            'conversation_id': conversation_id
        }

@tool
def SummarizeEmailThread():
    """ Generate a summary for the entire email thread """


def SummarizeEmailThreadNode(state: AgentState):
    """ Generate a summary for the entire email thread """

    # Example usage with single conversation
    # conversation_id = "AAQkADAwNDhkZDI3LThlODMtNDNkNy04ZGRjLWQwN2I1N2UxNjAyMAAQAFNQCQAw6C9Kgu8m2fpGE_c="  # This would come from Streamlit
    output_directory = "./summaries"

    messages = state.get("messages", [])

    if not messages:
        logger.warning("AGENTS/SUMMARY_AGENT - SummarizeEmailThreadNode() - No messages in state")
        return state
    
    # Get latest AI message
    last_message = messages[-1]

    if not isinstance(last_message, AIMessage) or not last_message.tool_calls:
        logger.warning("AGENTS/SUMMARY_AGENT - SummarizeEmailThreadNode() - Last message is not an AIMessage or has no tool calls")
        return state
    
    tool_call = last_message.tool_calls[0]
    
    try:
        email_id = state["email_context"].get("email_id", None)

        if not email_id:
            raise ValueError(f"email_id {email_id} is missing from state")
        
        conversation_id = fetch_emailId_from_conversationId(email_id=email_id)
        
        if not conversation_id:
            raise ValueError(f"conversation_id {conversation_id} was not found in the database")

        logger.info(f"=== Starting Email Thread Summarization for Conversation {conversation_id} ===")
        
        # Get or create summary for the conversation
        summary = get_or_create_thread_summary(conversation_id, output_directory)
        
        if summary['status'] == 'success':
            logger.info(f"Summary generated successfully for Conversation ID: {conversation_id}")
            
            success_message = ToolMessage(
                tool_call_id = tool_call.get("id"),
                content      = f"Summary generated successfully for conversation {conversation_id}"
            )

            state["conversation_summary"] = summary.get("summary")
            state["messages"].append(success_message)
        
        else:
            logger.error(f"Failed to generate summary: {summary.get('message')}")
            success_message = ToolMessage(
                tool_call_id = tool_call.get("id"),
                content      = f"Failed to generate summary for conversation {conversation_id} due to error: {summary.get('message')}"
            )
            
        logger.info("=== Process Complete ===")

    except Exception as e:
        logger.error(f"Fatal error occurred: {str(e)}", exc_info=True)
        raise

    finally:
        return state
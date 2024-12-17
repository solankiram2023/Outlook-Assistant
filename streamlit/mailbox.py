import streamlit as st
from datetime import datetime
import os
import logging
from openai import OpenAI
import tempfile
from audio_recorder_streamlit import audio_recorder
from dotenv import load_dotenv
from streamlit_quill import st_quill
from streamlit_chat import message
from email_service import EmailService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

query_params = st.query_params

token = query_params.get("access_token", None)
name = query_params.get("name", None)
preferred_username = query_params.get("preferred_username", None)

if "access_token" not in st.session_state:
    st.session_state["access_token"] = token

if "name" not in st.session_state:
    st.session_state["name"] = name

if "preferred_username" not in st.session_state:
    st.session_state["preferred_username"] = preferred_username

# Initialize EmailService
email_service = EmailService()

def text_to_speech(text, voice="alloy"):
    """Convert text to speech using OpenAI's TTS API"""
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        st.error("OpenAI API key not found. Please set OPENAI_API_KEY in your environment variables.")
        return None
        
    client = OpenAI(api_key=api_key)
    
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.mp3') as temp_audio:
            response = client.audio.speech.create(
                model="tts-1",
                voice=voice,
                input=text
            )
            response.stream_to_file(temp_audio.name)
            return temp_audio.name
    except Exception as e:
        st.error(f"Error generating speech: {str(e)}")
        return None

def record_and_transcribe():
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        st.error("OpenAI API key not found. Please set OPENAI_API_KEY in your environment variables.")
        return None

    st.markdown("### Record Audio")
    audio_bytes = audio_recorder(
        text="Click to record",
        recording_color="#e74c3c",
        neutral_color="#3498db",
        icon_name="microphone",
        icon_size="2x"
    )

    if audio_bytes:
        try:
            with st.spinner("Transcribing..."):
                with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as temp_audio:
                    temp_audio.write(audio_bytes)
                    temp_audio_path = temp_audio.name

                client = OpenAI(api_key=api_key)
                with open(temp_audio_path, "rb") as audio_file:
                    transcript = client.audio.transcriptions.create(
                        model="whisper-1",
                        file=audio_file
                    )

                os.unlink(temp_audio_path)
                
                if isinstance(transcript, dict) and "text" in transcript:
                    return transcript["text"]
                else:
                    st.error("Unexpected response format from OpenAI API.")
                    return None
        except Exception as e:
            st.error(f"Error during transcription: {str(e)}")
            return None

    return None

def get_initials(name):
    return ''.join(word[0].upper() for word in name.split() if word)

def get_category(email):

    category_colors = {
        'WORK': "#e8f0fe",        # Professional blue
        'MARKETING': "#e6f4ea",    # Light green
        'SOCIAL': "#f3e8fd",       # Light purple
        'UPDATES': "#fff0e0",      # Light orange
        'PERSONAL': "#fce8e6",     # Light red
        'BILLING': "#fdcfe8",      # Light pink
        'TRAVEL': "#ceead6",       # Mint green
        'EDUCATION': "#d2e3fc",    # Sky blue
        'HEALTH': "#d4e3d4",       # Sage green
        'PROFANITY': "#f28b82",    # Warning red
        'SPAM': "#e8eaed",         # Light grey
        'OTHER': "#f0f0f0"         # Default grey
    }

    try:
        # Get categories for this email from email service
        response = email_service.get_email_category(email['id'])
        
        if response["status"] == 200 and response["data"]:
            # Process all categories from response
            categories = []
            for category in response["data"]:
                category_upper = category.upper()
                categories.append({
                    'name': category_upper,
                    'color': category_colors.get(category_upper, category_colors['OTHER'])
                })
            return categories
        
        # Return default if no categories found
        return [{'name': 'OTHER', 'color': category_colors['OTHER']}]
        
    except Exception as e:
        logger.error(f"Error getting categories: {str(e)}")
        return [{'name': 'OTHER', 'color': category_colors['OTHER']}]

def initialize_session_state():
    if 'show_chatbot' not in st.session_state:
        st.session_state.show_chatbot = False
    if 'chat_messages' not in st.session_state:
        st.session_state.chat_messages = []
    if 'emails' not in st.session_state:
        st.session_state.emails = []
    if 'selected_email_id' not in st.session_state:
        st.session_state.selected_email_id = None
    if 'show_menu' not in st.session_state:
        st.session_state.show_menu = False
    if 'transcribed_text' not in st.session_state:
        st.session_state.transcribed_text = ""
    if st.button("💬 Chat"):
        st.session_state.show_chat = not st.session_state.get('show_chat', False)
        

# Fetch emails and update session state
def fetch_emails(email_service):
    with st.spinner(f'Fetching emails from {st.session_state.selected_folder}......'):
        response = email_service.fetch_emails(folder=st.session_state.selected_folder)
        if response["status"] == 200:
            emails_data = response["data"]
            logger.info(f"Processing {len(emails_data)} emails from {st.session_state.selected_folder}")
            st.session_state.emails = [
                {
                    "id": email["email_id"],
                    "sender": email["sender_name"],
                    "email": email["sender_email"],
                    "subject": email["subject"],
                    "content": email["body_preview"] if email.get("body_preview") else "",
                    "date": email["received_datetime"],
                    "read": email.get("is_read", False),
                    "starred": False,
                    "category": "Work",
                    "attachments": email_service.load_attachments(email["email_id"]) if email.get("has_attachments") else []
                }
                for email in emails_data
            ]
            logger.info(f"Processed {len(st.session_state.emails)} emails")
        else:
            st.error(f"Failed to fetch emails: {response['message']}")
            logger.error(f"Failed to fetch emails: {response['message']}")

# Load full email content
def load_email_content(email_id):
    """
    Load and process email content from the API
    Returns None if there's an error loading the email
    """
    try:
        email_response = email_service.load_email(email_id)
        if email_response["status"] == 200:
            email_data = email_response["data"]
            
            # Find and update the email in session state
            for idx, email in enumerate(st.session_state.emails):
                if email['id'] == email_id:
                    # Create a new dictionary with updated content
                    st.session_state.emails[idx] = {
                        **email,  # Preserve existing email data
                        'content': email_data.get('body', ''),  # Safely get body content
                        'attachments': email_data.get('attachments', []),
                        'read': True  # Mark as read
                    }
                    return email_data
        else:
            logger.error(f"Failed to load email {email_id}: {email_response.get('message', 'Unknown error')}")
            return None
    except Exception as e:
        logger.error(f"Error loading email {email_id}: {str(e)}")
        return None


# Render email list

def render_email_list():

    # Header with title and refresh button
    col1, col2 = st.columns([8, 1]) 

    with col1:
        st.markdown(f"### {st.session_state.selected_folder}")

    with col2:
        if st.button("🔄", key="refresh_button"):
            # Call fetch_emails function to reload the email list
            response = email_service.fetch_emails(st.session_state.selected_folder)
            if response["status"] == 200:
                # Update session state emails
                st.session_state.emails = [
                    {
                        "id": email["email_id"],
                        "sender": email["sender_name"],
                        "email": email["sender_email"],
                        "subject": email["subject"],
                        "content": email["body_preview"] if email.get("body_preview") else "",
                        "date": email["received_datetime"],
                        "read": email.get("is_read", False),
                        "starred": False,
                        "category": "Work",
                        "attachments": email_service.load_attachments(email["email_id"]) if email.get("has_attachments") else []
                    }
                    for email in response["data"]
                ]
                # Set a success flag in session state
                st.session_state.refresh_success = True
            else:
                # Handle error, optionally set an error flag
                st.session_state.refresh_success = False
                st.error(f"Failed to refresh emails: {response['message']}")

    
    # Search Input Field
    search_query = st.text_input(
        label="Search",
        label_visibility="hidden",
        key="email_search", 
        placeholder="Search mail..."
    ).lower()
    
    st.session_state.search_query = search_query

    # Filter emails based on search query
    filtered_emails = st.session_state.get("emails", [])
    if search_query:
        filtered_emails = []
        for email in st.session_state.get("emails", []):
            # Get categories for the email
            email_categories = get_category(email)
            # Convert categories to lowercase for case-insensitive search
            category_names = [cat['name'].lower() for cat in email_categories]
            
            # Check if search query matches any email field or categories
            if (search_query in email.get("sender", "").lower() or
                search_query in email.get("email", "").lower() or
                search_query in email.get("subject", "").lower() or
                search_query in email.get("content", "").lower() or
                any(search_query in cat_name for cat_name in category_names)):
                filtered_emails.append(email)

    # Email List Container
    email_list_container = st.container()

    # Render emails in a structured format
    with email_list_container:
        if not filtered_emails:
            if search_query:
                st.info("No emails match your search.")
            else:
                st.info("No emails found. Please refresh or try again.")
        else:
            for idx, email in enumerate(filtered_emails):
                categories = get_category(email)
                initials = get_initials(email["sender"])

                with st.container():
                    cols = st.columns([8, 2])  

                    # Left Column: Email Preview
                    with cols[0]:
                        preview_content = email["content"][:50] + "..." if email["content"] else "No preview available"

                        category_tags_html = "".join([
                            f'<span class="category-tag" style="display: inline-block; margin-right: 5px; '
                            f'padding: 2px 8px; font-size: 11px; border-radius: 12px; '
                            f'background-color: {cat["color"]}; color: #333;">'
                            f'{cat["name"]}</span>'
                            for cat in categories
                        ])

                        email_html = f"""
                        <div class="email-list-item" style="margin-bottom: 10px;">
                            <div class="profile-pic" style="background-color: {categories[0]['color']}; width: 50px; height: 50px; border-radius: 25px; display: inline-block; text-align: center; line-height: 50px;">
                                <span style="color: gray; font-size: 14px;">{initials}</span>
                            </div>
                            <div class="email-content-preview" style="display: inline-block; margin-left: 10px; vertical-align: top; width: calc(100% - 70px);">
                                <div class="sender-name" style="font-weight: bold; font-size: 14px;">{email['sender']}</div>
                                <div class="email-subject" style="color: #555; font-size: 12px;">{email['subject']}</div>
                                <div style="font-size: 12px; color: #777;">{preview_content}</div>
                                <div class="category-tags" style="margin-top: 5px;">
                                    {category_tags_html}
                                </div>
                            </div>
                        </div>
                        """

                        if email.get("starred"):
                            email_html += '<div class="flag-icon">🚩</div>'

                        st.markdown(email_html, unsafe_allow_html=True)

                    # Right Column: View Button
                    with cols[1]:
                        if st.button("View", key=f"select_{email['id']}_{idx}", use_container_width=True):
                            st.session_state.selected_email_id = email["id"]
                            load_email_content(email["id"])
                            logger.info(f"Stored selected_email_id: {email['id']}")
                            st.rerun()


# Render selected email
def render_selected_email():
    """
    Render the selected email with proper error handling and content processing
    """
    if not st.session_state.selected_email_id:
        return

    # Load email content
    email_data = load_email_content(st.session_state.selected_email_id)
    
    if not email_data:
        st.error("Unable to load email content. Please try again.")
        return

    # Create a container for the entire email view
    with st.container():
        # Header section with three columns for subject, TTS, and menu
        header_col1, header_col2, header_col3 = st.columns([7, 2, 1])
        
        with header_col1:
            st.markdown(f"### {email_data.get('subject', 'No Subject')}")
        
        # Text-to-speech controls
        with header_col2:
            voice_options = {"Alloy": "alloy", "Echo": "echo", "Fable": "fable"}
            selected_voice = st.selectbox(
                "Voice",
                options=list(voice_options.keys()),
                label_visibility="collapsed"
            )
            
            if st.button("🔊 Read Email"):
                email_text = (
                    f"Email from {email_data.get('sender_email', 'Unknown sender')}. "
                    f"Subject: {email_data.get('subject', 'No subject')}. "
                    f"Content: {email_data.get('body', 'No content')}"
                )
                
                with st.spinner("Generating audio..."):
                    audio_file = text_to_speech(email_text, voice=voice_options[selected_voice])
                    if audio_file:
                        try:
                            with open(audio_file, 'rb') as f:
                                audio_bytes = f.read()
                            st.audio(audio_bytes, format='audio/mp3')
                        finally:
                            # Clean up temp file
                            if os.path.exists(audio_file):
                                os.remove(audio_file)
        
        # Close button
        with header_col3:
            if st.button("✕"):
                del st.session_state.selected_email_id
                st.rerun()

        # Metadata section with safe gets
        st.markdown(f"**From:** {email_data.get('sender_email', 'Unknown sender')}")
        try:
            date_str = datetime.fromisoformat(email_data.get('received_datetime', '')).strftime('%b %d, %Y %I:%M %p')
        except (ValueError, TypeError):
            date_str = 'Unknown date'
        st.markdown(f"**Date:** {date_str}")
        st.markdown("---")

        # Render email content
        email_content = email_data.get('body', 'No content available')
        attachments = email_data.get('attachments', [])

        # Display email body
        st.markdown(f"#### Email Content:")
        st.markdown(email_content)

        # Display attachments
        if attachments:
            st.markdown("#### Attachments:")
            for attachment in attachments:
                if isinstance(attachment, dict):
                    # Retrieve attachment details
                    name = attachment.get('name', 'Unnamed Attachment')
                    url = attachment.get('bucket_url') or attachment.get('download_url', '#')
                    size = attachment.get('size', 'Unknown size')
                    content_type = attachment.get('content_type', 'Unknown type')

                    # Render attachment details
                    st.markdown(
                        f"- 📎 **[{name}]({url})** ({size}, {content_type})",
                        unsafe_allow_html=True
                    )
                else:
                    # Handle case where attachment is a simple string URL
                    st.markdown(f"- 📎 **[Download Attachment]({attachment})**", unsafe_allow_html=True)


        # Reply section
        st.markdown("---")
        st.markdown("#### Reply")
        
        # Replace the st_quill with custom reply form
        reply_to = email_data.get('sender_email', '')
        reply_subject = f"Re: {email_data.get('subject', 'No Subject')}"
        original_body = email_data.get('body', '')
        
        # To field
        st.text_input("To:", value=reply_to, key="reply_to")
        
        # Subject field
        st.text_input("Subject:", value=reply_subject, key="reply_subject")
        
        # Body field with formatting toolbar
        st.markdown("""
        <div style="border-bottom: 1px solid #ddd; padding: 8px 0; margin-bottom: 8px;">
            <button style="margin-right: 8px; padding: 4px 8px;">B</button>
            <button style="margin-right: 8px; padding: 4px 8px;"><i>I</i></button>
            <button style="margin-right: 8px; padding: 4px 8px;"><u>U</u></button>
            <select style="padding: 4px 8px;">
                <option>Sans Serif</option>
                <option>Serif</option>
            </select>
        </div>
        """, unsafe_allow_html=True)
        
        reply_content = st.text_area(
            "Body:",
            value="",
            height=300,
            key="reply_body",
            label_visibility="collapsed"
        )
        
        # Buttons
        col1, col2, col3 = st.columns([2, 2, 8])  
        with col1:
            if st.button("Send", type="primary", use_container_width=True):  
                # Handle send logic here
                pass
        with col2:
            if st.button("Save Draft", use_container_width=True):  
                # Handle draft saving logic here
                pass
                    
# Main mailbox function
def render_chat_window():
    """Render chat window with Q&A style interface and voice input."""
    # Header with title, mic button, and close button
    col1, col2, col3 = st.columns([5, 1, 1])
    with col1:
        st.title("Chat Assistant")
    
    # Add microphone button in middle column
    with col2:
        audio_bytes = audio_recorder(
            text="🎤",
            recording_color="#e74c3c",
            neutral_color="#3498db",
            icon_size="1x"
        )
    
    # Close button in last column
    with col3:
        if st.button("✕", key="close_chat"):
            st.session_state.show_chat = False
            st.rerun()

    # Initialize messages if not exist
    if "messages" not in st.session_state:
        st.session_state.messages = []
    
    # Display chat messages
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
    
    # Chat input container
    chat_input_container = st.empty()
    user_question = chat_input_container.chat_input("Type your message...")
    
    # Handle audio transcription
    if audio_bytes and 'audio_processed' not in st.session_state:
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as temp_audio:
                temp_audio.write(audio_bytes)
                temp_audio_path = temp_audio.name

            with st.spinner("Transcribing..."):
                api_key = os.getenv('OPENAI_API_KEY')
                if not api_key:
                    st.error("OpenAI API key not found.")
                    return
                
                client = OpenAI(api_key=api_key)
                
                with open(temp_audio_path, "rb") as audio_file:
                    transcript = client.audio.transcriptions.create(
                        model="whisper-1",
                        file=audio_file
                    )

            os.unlink(temp_audio_path)

            if hasattr(transcript, 'text'):
                # Instead of directly adding to messages, set the chat input value
                chat_input_container.empty()  # Clear the previous input
                user_question = transcript.text
                st.session_state.audio_processed = True

        except Exception as e:
            st.error(f"Error during transcription: {str(e)}")
            logger.error(f"Audio transcription error: {str(e)}")
    
    # Handle both text and transcribed input
    if user_question:
        st.session_state.messages.append({
            "role": "user", 
            "content": user_question
        })
        response = email_service.send_user_prompt(
            st.session_state["preferred_username"], 
            user_question, 
            st.session_state.selected_email_id
        )
        logger.info(f"Response from the chat window: {response}")
        st.session_state.messages.append({
            "role": "assistant",
            "content": response
        })
        st.rerun()
    
    # Clear audio_processed flag when no audio is being recorded
    if not audio_bytes and 'audio_processed' in st.session_state:
        del st.session_state.audio_processed

# Update your existing render_mailbox function
def render_mailbox():
    initialize_session_state()
    fetch_emails(email_service)

    if not st.session_state.get('show_chat', False):
        # Regular email interface
        col1, col2 = st.columns([1, 2])
        with col1:
            render_email_list()
        with col2:
            render_selected_email()
    else:
        # Show chat interface
        render_chat_window()

    st.markdown("---")
    st.markdown(
        f'<div class="refresh-time">Last refreshed at {datetime.now().strftime("%I:%M %p")}</div>',
        unsafe_allow_html=True
    )
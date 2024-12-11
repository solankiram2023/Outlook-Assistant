# mailbox.py
import streamlit as st
from datetime import datetime
import os
import logging
from openai import OpenAI
import tempfile
from audio_recorder_streamlit import audio_recorder
from dotenv import load_dotenv
from streamlit_quill import st_quill

from email_service import EmailService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Load environment variables
load_dotenv()

# Initialize EmailService
email_service = EmailService()


def text_to_speech(text, voice="alloy"):
    """Convert text to speech using OpenAI's TTS API"""
    
    # Initialize OpenAI client with API key
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        st.error("OpenAI API key not found. Please set OPENAI_API_KEY in your environment variables.")
        return None
        
    client = OpenAI(api_key=api_key)
    
    try:
        # Create temporary file for audio
        with tempfile.NamedTemporaryFile(delete=False, suffix='.mp3') as temp_audio:
            # Generate speech using OpenAI's TTS
            response = client.audio.speech.create(
                model="tts-1",
                voice=voice,
                input=text
            )
            
            # Save to temporary file
            response.stream_to_file(temp_audio.name)
            return temp_audio.name
            
    except Exception as e:
        st.error(f"Error generating speech: {str(e)}")
        return None

def record_and_transcribe():
    # Initialize OpenAI client with API key
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        st.error("OpenAI API key not found. Please set OPENAI_API_KEY in your environment variables.")
        return None

    client = OpenAI(api_key=api_key)

    # Add the audio recorder
    st.markdown("### Record Audio")
    audio_bytes = audio_recorder(
        text="Click to record",
        recording_color="#e74c3c",
        neutral_color="#3498db",
        icon_name="microphone",
        icon_size="2x"
    )

    # Process the audio if recorded
    if audio_bytes:
        try:
            # Save audio to a temporary file
            with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as temp_audio:
                temp_audio.write(audio_bytes)
                temp_audio_path = temp_audio.name

            # Show a status message
            with st.spinner("Transcribing..."):
                # Transcribe using OpenAI Whisper API
                with open(temp_audio_path, "rb") as audio_file:
                    transcript = client.audio.transcriptions.create(
                        model="whisper-1",
                        file=audio_file
                    )

            # Clean up the temporary file
            os.unlink(temp_audio_path)

            # Access transcription text (correctly handle transcript object)
            if isinstance(transcript, dict) and "text" in transcript:
                return transcript["text"]
            else:
                st.error("Unexpected response format from OpenAI API.")
                return None

        except Exception as e:
            st.error(f"Error during transcription: {str(e)}")
            return None

    return None


# Function to get initials from name
def get_initials(name):
    return ''.join(word[0].upper() for word in name.split() if word)

# Function to get category color
def get_category(email):
    categories = {
        'Work'              : ("Work", "#e8f0fe"),
        'Client Issue'      : ("Client Issue", "#fce8e6"),
        'Marketing'         : ("Marketing", "#e6f4ea"),
        'IT Support'        : ("IT Support", "#fff0e0"),
        'Legal/Contracts'   : ("Legal/Contracts", "#f3e8fd"),
        'HR/Benefits'       : ("HR/Benefits", "#f7f8fb")
    }
    
    category_name, color = categories.get(email['category'], ("Other", "#f0f0f0"))
    return category_name, color

# Initialize session state
def initialize_session_state():
    if 'emails' not in st.session_state:
        st.session_state.emails = []
    if 'selected_email_id' not in st.session_state:
        st.session_state.selected_email_id = None
    if 'show_menu' not in st.session_state:
        st.session_state.show_menu = False
    if 'transcribed_text' not in st.session_state:
        st.session_state.transcribed_text = ""

# Fetch emails and update session state
def fetch_emails(email_service):
    with st.spinner('Fetching emails...'):
        response = email_service.fetch_emails()
        if response["status"] == 200:
            emails_data = response["data"]
            logger.info(f"Processing {len(emails_data)} emails")
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
    email_response = email_service.load_email(email_id)
    if email_response["status"] == 200:
        email_data = email_response["data"]
        for email in st.session_state.emails:
            if email['id'] == email_id:
                email['content'] = email_data['body']
                email['attachments'] = email_data.get('attachments', [])

# Render email list
def render_email_list():
    st.markdown("### Inbox")

    # Add audio recorder with transcription
    st.write("Search by voice:")

    # Record and Clear Buttons
    col_record, col_clear = st.columns([4, 1])

    with col_record:
        transcribed_text = record_and_transcribe()
        if transcribed_text:
            st.session_state.transcribed_text = transcribed_text

    with col_clear:
        if st.button("Clear", use_container_width=True):
            st.session_state.transcribed_text = ""

    # Search Input Field
    st.text_input(
        label="Search",
        label_visibility="hidden",
        key="search",
        placeholder="Search mail...",
        value=st.session_state.get("transcribed_text", "")
    )

    # Email List Container
    email_list_container = st.container()

    # Render emails in a structured format
    with email_list_container:
        for idx, email in enumerate(st.session_state.get("emails", [])):  # Use enumerate for unique indexing
            category, bg_color = get_category(email)
            initials = get_initials(email["sender"])

            with st.container():
                cols = st.columns([8, 2])  # Adjusted column sizes for better alignment

                # Left Column: Email Preview
                with cols[0]:
                    preview_content = email["content"][:50] + "..." if email["content"] else "No preview available"

                    email_html = f"""
                    <div class="email-list-item" style="margin-bottom: 10px;">
                        <div class="profile-pic" style="background-color: {bg_color}; width: 50px; height: 50px; border-radius: 25px; display: inline-block; text-align: center; line-height: 50px;">
                            <span style="color: gray; font-size: 14px;">{initials}</span>
                        </div>
                        <div class="email-content-preview" style="display: inline-block; margin-left: 10px; vertical-align: top; width: calc(100% - 70px);">
                            <div class="sender-name" style="font-weight: bold; font-size: 14px;">{email['sender']}</div>
                            <div class="email-subject" style="color: #555; font-size: 12px;">{email['subject']}</div>
                            <div style="font-size: 12px; color: #777;">{preview_content}</div>
                            <div class="category-tag" style="margin-top: 5px; display: inline-block; padding: 2px 5px; font-size: 10px; border-radius: 3px; background-color: {bg_color}; color: white;">
                                {category}
                            </div>
                        </div>
                    </div>
                    """

                    if email.get("starred"):
                        email_html += '<div class="flag-icon">🚩</div>'

                    st.markdown(email_html, unsafe_allow_html=True)

                # Right Column: View Button
                with cols[1]:
                    # Ensure unique key using idx from enumerate
                    if st.button("View", key=f"select_{email['id']}_{idx}", use_container_width=True):
                        st.session_state.selected_email_id = email["id"]
                        load_email_content(email["id"])
                        st.rerun()



# Render selected email
def render_selected_email():
    if st.session_state.selected_email_id:
        selected_email = next(
            (email for email in st.session_state.emails 
             if email['id'] == st.session_state.selected_email_id),
            None
        )

        if selected_email:
            header_col1, header_col2, header_col3 = st.columns([8, 2, 1])

            with header_col1:
                st.markdown(f"### {selected_email['subject']}")

            with header_col2:
                voice_options = {"Alloy": "alloy", "Echo": "echo", "Fable": "fable"}
                selected_voice = st.selectbox(
                    "Voice",
                    options=list(voice_options.keys()),
                    label_visibility="collapsed"
                )
                if st.button("🔊 Read Email"):
                    email_text = f"Email from {selected_email['sender']}. Subject: {selected_email['subject']}. Content: {selected_email['content']}"

                    with st.spinner("Generating audio..."):
                        audio_file = text_to_speech(email_text, voice=voice_options[selected_voice])
                        if audio_file:
                            with open(audio_file, 'rb') as f:
                                audio_bytes = f.read()
                            st.audio(audio_bytes, format='audio/mp3')
                            os.remove(audio_file)

            with header_col3:
                if st.button("⋮", key="menu_button"):
                    st.session_state.show_menu = not st.session_state.show_menu

                if st.session_state.show_menu:
                    st.markdown("""
                        <div class="dropdown-container">
                            <div class="dropdown-item">Archive</div>
                            <div class="dropdown-item">Delete</div>
                            <div class="dropdown-item">Mark as unread</div>
                            <div class="dropdown-item">Snooze</div>
                        </div>
                    """, unsafe_allow_html=True)

            st.markdown(f"**From:** {selected_email['sender']} <{selected_email['email']}>")
            st.markdown(f"**Date:** {datetime.fromisoformat(selected_email['date']).strftime('%b %d, %Y %I:%M %p')}")

            st.markdown("---")
            content_with_newlines = selected_email['content'].replace('\n', '<br>')
            st.markdown(content_with_newlines, unsafe_allow_html=True)

            if selected_email.get('attachments'):
                st.markdown("---")
                st.markdown("**Attachments:**")
                for attachment in selected_email['attachments']:
                    st.markdown(f"- {attachment}")

            st.markdown("---")
            st.markdown("#### Reply")
            st_quill(placeholder="Reply to this email...")
            col_reply1, col_reply2 = st.columns([2, 13])

            with col_reply1:
                st.button("Send", type="primary")

            with col_reply2:
                st.button("Save as Draft")
        else:
            st.markdown("### Select an email to read")

# Main mailbox function
def render_mailbox():

    initialize_session_state()
    fetch_emails(email_service)

    col1, col2 = st.columns([1, 2])

    with col1:
        render_email_list()

    with col2:
        render_selected_email()

    st.markdown("---")
    st.markdown(
        f'<div class="refresh-time">Last refreshed at {datetime.now().strftime("%I:%M %p")}</div>',
        unsafe_allow_html=True
    )

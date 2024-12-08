# mailbox_ui.py
import streamlit as st
from datetime import datetime
from streamlit_quill import st_quill
from components import get_initials, get_category
from email_service import EmailService
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_attachments(email_id: str) -> list:
    """Load attachments for a specific email."""
    try:
        email_response = email_service.load_email(email_id)
        if email_response["status"] == 200 and email_response["data"].get("attachments"):
            return email_response["data"]["attachments"]
        return []
    except Exception as e:
        logger.error(f"Error loading attachments for email {email_id}: {str(e)}")
        return []

# Initialize EmailService
email_service = EmailService()

st.set_page_config(layout="wide")

# Custom CSS
with open("style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# Initialize session state
if 'emails' not in st.session_state:
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
                    "attachments": load_attachments(email["email_id"]) if email.get("has_attachments") else []
                }
                for email in emails_data
            ]
            logger.info(f"Processed {len(st.session_state.emails)} emails")
        else:
            st.error(f"Failed to fetch emails: {response['message']}")
            st.session_state.emails = []
            logger.error(f"Failed to fetch emails: {response['message']}")

def refresh_emails():
    with st.spinner('Refreshing emails...'):
        logger.info("Refreshing emails...")
        response = email_service.fetch_emails()
        if response["status"] == 200:
            emails_data = response["data"]
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
                    "attachments": load_attachments(email["email_id"]) if email.get("has_attachments") else []
                }
                for email in emails_data
            ]
            st.success("Emails refreshed successfully!")
            logger.info("Emails refreshed successfully")
        else:
            st.error(f"Failed to refresh emails: {response['message']}")
            logger.error(f"Failed to refresh emails: {response['message']}")

if 'selected_email_id' not in st.session_state:
    st.session_state.selected_email_id = None

if 'show_menu' not in st.session_state:
    st.session_state.show_menu = False

# Sidebar
with st.sidebar:
    st.button("Compose", type="primary", use_container_width=True)
    st.markdown("---")
    
    nav_options = {
        "Inbox": "inbox",
        "Starred": "starred",
        "Snoozed": "snoozed",
        "Sent": "sent",
        "Drafts": "drafts",
        "Trash": "trash"
    }
    
    selected_nav = None
    for label, value in nav_options.items():
        if st.button(label, key=f"nav_{value}", use_container_width=True):
            selected_nav = value

# Main layout
col1, col2 = st.columns([1, 2])

# Email list column
with col1:
    st.markdown("### Inbox")
    
    search_query = st.text_input(
        label="Search",
        label_visibility="hidden",
        key="search",
        placeholder="Search mail..."
    )
    
    email_list_container = st.container()
    
    # Email list display
    with email_list_container:
        for email in st.session_state.emails:
            category, bg_color = get_category(email)
            initials = get_initials(email['sender'])

            with st.container():
                cols = st.columns([6, 1])

                with cols[0]:
                    preview_content = email['content'][:50] + "..." if email['content'] else "No preview available"
                    
                    email_html = f"""
                    <div class="email-list-item">
                        <div class="profile-pic" style="background-color: {bg_color}">
                            <span style="color: gray">{initials}</span>
                        </div>
                        <div class="email-content-preview">
                            <div class="sender-name">{email['sender']}</div>
                            <div class="email-subject">{email['subject']}</div>
                            <div>{preview_content}</div>
                            <div class="category-tag" style="background-color: {bg_color}">
                                {category}
                            </div>
                        </div>
                    """

                    if email.get('starred'):
                        email_html += '<div class="flag-icon">🚩</div>'

                    email_html += "</div>"
                    st.markdown(email_html, unsafe_allow_html=True)
                
                with cols[1]:
                    if st.button("View", key=f"select_{email['id']}"):
                        st.session_state.selected_email_id = email['id']
                        # Fetch full email content
                        email_response = email_service.load_email(email['id'])
                        if email_response["status"] == 200:
                            email_data = email_response["data"]
                            # Update session state with full email data
                            for e in st.session_state.emails:
                                if e['id'] == email['id']:
                                    e['content'] = email_data['body']
                                    e['attachments'] = email_data.get('attachments', [])
                        st.rerun()

# Email content column
with col2:
    if st.session_state.selected_email_id:
        selected_email = next(
            (email for email in st.session_state.emails 
             if email['id'] == st.session_state.selected_email_id),
            None
        )
        
        if selected_email:
            header_col1, header_col2 = st.columns([10, 1])
            
            with header_col1:
                st.markdown(f"### {selected_email['subject']}")
            
            with header_col2:
                menu_container = st.container()
                
                with menu_container:
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
            content = st_quill(placeholder="Reply to this email...")
            col_reply1, col_reply2 = st.columns([1, 13])
            
            with col_reply1:
                st.button("Send", type="primary")
            
            with col_reply2:
                st.button("Save as Draft")
    else:
        st.markdown("### Select an email to read")

# Footer
st.markdown("---")
st.markdown(
    f'<div class="refresh-time">Last refreshed at {datetime.now().strftime("%I:%M %p")}</div>',
    unsafe_allow_html=True
)
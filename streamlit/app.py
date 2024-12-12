# streamlit_app.py
import streamlit as st
from datetime import datetime
from mailbox import render_mailbox
from email_service import EmailService

# Initialize EmailService
email_service = EmailService()

st.set_page_config(layout="wide")

# Custom CSS
with open("style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# Initialize session state for selected folder
if 'selected_folder' not in st.session_state:
    st.session_state.selected_folder = 'Inbox'  # Default folder

def get_folder_count(folder_name):
    """Get the count of emails in a folder"""
    response = email_service.fetch_emails(folder=folder_name)
    if response["status"] == 200 and "data" in response:
        return len(response["data"])
    return 0

def render_sidebar():
    with st.sidebar:
        st.button("✉️ Compose", type="primary", use_container_width=True)
        st.markdown("---")

        nav_options = {
            "Inbox": {"icon": "📥", "value": "Inbox"},
            "Archive": {"icon": "📁", "value": "Archive"},
            "Sent Items": {"icon": "📤", "value": "Sent Items"},
            "Drafts": {"icon": "📝", "value": "Drafts"},
            "Deleted Items": {"icon": "🗑️", "value": "Deleted Items"},
            "Junk Email": {"icon": "⚠️", "value": "Junk Email"},
            "Important": {"icon": "⭐", "value": "Important"},
            "Conversation History": {"icon": "💬", "value": "Conversation History"},
            "Outbox": {"icon": "📨", "value": "Outbox"}
        }

        for label, details in nav_options.items():
            col1, col2 = st.columns([7, 1])  # Adjust ratio as needed
            
            # Create button with icon and label
            with col1:
                button_text = f"{details['icon']} {label}"
                if st.button(button_text, key=f"nav_{details['value']}", use_container_width=True):
                    st.session_state.selected_folder = details['value']
                    st.rerun()
            
            # Show count if greater than 0
            with col2:
                count = get_folder_count(details['value'])
                if count > 0:
                    st.markdown(f"<div class='folder-count'>{count}</div>", unsafe_allow_html=True)

if __name__ == "__main__":
    render_sidebar()
    render_mailbox()
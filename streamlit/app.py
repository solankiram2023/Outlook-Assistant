# streamlit_app.py
import streamlit as st
from datetime import datetime
from mailbox import render_mailbox

st.set_page_config(layout="wide")

# Custom CSS
with open("style.css") as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

# Sidebar
def render_sidebar():
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

        for label, value in nav_options.items():
            st.button(label, key=f"nav_{value}", use_container_width=True)

if __name__ == "__main__":
    render_sidebar()
    render_mailbox()
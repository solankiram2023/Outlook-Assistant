import streamlit as st
from dotenv import load_dotenv
import os
import webbrowser

# Load environment variables from .env
load_dotenv()

# Sign-In Page
def sign_in_page():
    col1, col2 = st.columns([6, 1]) 

    with col1:
        st.title("Outlook Email Management Assistant")

    with col2:
        if st.button("Sign In"):
            # Open the redirect URL in the user's browser
            st.write(f"Opening the Microsoft sign-in page")
            base_url = os.getenv("FASTAPI_URL")
            webbrowser.open(f"{base_url}{os.getenv('SIGN_IN_ENDPOINT')}")

# Run the app
if __name__ == "__main__":
    st.set_page_config(page_title="Sign-In Page")
    sign_in_page()

import os
import tempfile
from openai import OpenAI
import streamlit as st
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

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
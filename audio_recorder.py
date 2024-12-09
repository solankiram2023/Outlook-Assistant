import os
import tempfile
import streamlit as st
from openai import OpenAI
from audio_recorder_streamlit import audio_recorder
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def record_and_transcribe():
    # Initialize OpenAI client with API key
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        st.error("OpenAI API key not found. Please set OPENAI_API_KEY in your environment variables.")
        return None
        
    client = OpenAI(api_key=api_key)
    
    # Add the audio recorder
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
            with tempfile.NamedTemporaryFile(delete=False, suffix='.wav') as temp_audio:
                temp_audio.write(audio_bytes)
                temp_audio_path = temp_audio.name

            # Show a status message
            with st.spinner("Transcribing..."):
                # Transcribe using OpenAI Whisper with new API
                with open(temp_audio_path, "rb") as audio_file:
                    transcript = client.audio.transcriptions.create(
                        model="whisper-1",
                        file=audio_file
                    )

            # Clean up the temporary file
            os.unlink(temp_audio_path)
            
            # Return the transcribed text
            return transcript.text

        except Exception as e:
            st.error(f"Error during transcription: {str(e)}")
            return None

    return None
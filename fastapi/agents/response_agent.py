import os
from typing import Dict, Optional
import json
import logging
import requests
from openai import OpenAI
from dotenv import load_dotenv
import markdown2
from langchain.tools import tool
from langchain_core.messages import SystemMessage, AIMessage, ToolMessage
from agents.state import AgentState

# Load environment variables
load_dotenv()

# Initialize logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AutoResponseGenerator:
    def __init__(self):
        self.client = OpenAI(
            api_key      = os.getenv("OPENAI_API_KEY"),
            project      = os.getenv("PROJECT_ID"),
            organization = os.getenv("ORGANIZATION_ID")
        )
        
    def _generate_response_prompt(self, email_data: Dict, user_prompt: str) -> str:
        """Generate the prompt for OpenAI based on email context and user input"""
        
        # Get full name from sender_name if available, otherwise use email
        recipient_name = email_data.get('sender_name', email_data.get('sender_email').split('@')[0])
        # Get responder's name (the user's name)
        sender_name = email_data.get('recipient_name', email_data.get('recipient_email').split('@')[0])
        
        return f"""
            You are an AI assistant helping to draft an email response.
            
            Original Email Details:
            From: {email_data.get('sender_email')}
            Sender's Full Name: {recipient_name}
            Subject: {email_data.get('subject')}
            Content: {email_data.get('body')}

            Your Information (for signature):
            Your Name: {sender_name}

            User's Response Instructions:
            {user_prompt}
            
            Please provide:
            1. An appropriate subject line (formatted as 'Subject: Your Subject Here')
            2. A professional email response that:
            - Uses the sender's full name in the greeting (e.g., "Dear [Full Name],")
            - Maintains a professional tone
            - Incorporates the user's specified response details
            - Keeps the response concise but complete
            - Ends with exactly:
                
                Thanks & Regards,
                {sender_name}
            
            Format your response as:
            Subject: [Your subject line]

            [Your email content including greeting and the exact signature format specified above]

            Note: Do not include any placeholders like [Your Name] or [Your Position]. Use the exact signature format provided.
        """

    def _parse_response(self, response: str) -> Dict[str, str]:
        """Parse the generated response to separate subject and content"""
        lines = response.strip().split('\n')
        subject = ""
        content = []
        
        # Find the subject line
        for i, line in enumerate(lines):
            if line.lower().startswith('subject:'):
                subject = line[8:].strip()  # Remove "Subject: " prefix
                content = lines[i+1:]  # Get remaining lines as content
                break
        
        if not subject:  # If no subject found, use a default format
            subject = "Re: " + content[0] if content else "Re: No Subject"
            
        return {
            "subject": subject,
            "content": '\n'.join(content).strip()
        }

    def _convert_to_html(self, text: str) -> str:
        """Convert text to HTML using markdown2"""
        
        # Convert markdown to HTML
        html = markdown2.markdown(text)
        
        # Wrap in styled div
        return f"""
        <div style="font-family: Arial, sans-serif; font-size: 14px; line-height: 1.6; color: #333;">
            {html}
        </div>
        """

    def _validate_response(self, response: Dict[str, str]) -> bool:
        """Validate the generated response using guardrails"""
        
        if not response.get("subject") or not response.get("content"):
            return False
            
        if len(response["content"]) < 10:
            return False
            
        # Add more validation rules as needed
        inappropriate_terms = ['INSERT_INAPPROPRIATE_TERMS_HERE']
        if any(term in response["content"].lower() for term in inappropriate_terms):
            return False
            
        return True

    def generate_preview(self, email_data: Dict, user_prompt: str) -> Optional[Dict]:
        """Generate a preview of the response without sending"""
        
        try:
            prompt = self._generate_response_prompt(email_data, user_prompt)
            
            response = self.client.chat.completions.create(
                model    = "gpt-4o",
                messages = [
                    {"role": "system", "content": "You are a professional email assistant."},
                    {"role": "user", "content": prompt}
                ],
                temperature = 0.7,
                max_tokens  = 500
            )
            
            generated_response = response.choices[0].message.content
            parsed_response = self._parse_response(generated_response)
            
            if self._validate_response(parsed_response):
                html_content = self._convert_to_html(parsed_response["content"])
                return {
                    "subject": parsed_response["subject"],
                    "plain_text": parsed_response["content"],
                    "html_content": html_content,
                    "preview": True
                }
            
            return None
                
        except Exception as e:
            logger.error(f"Error generating response preview: {str(e)}")
            return None

    def generate_response(self, email_data: Dict, user_prompt: str) -> Optional[Dict]:
        """Generate final HTML-formatted email response"""
        
        preview = self.generate_preview(email_data, user_prompt)
        
        if preview:
            preview["preview"] = False
            return preview
        
        return None

class EmailSender:
    
    def __init__(self, access_token: str):
        self.access_token = access_token
        self.graph_api_endpoint = os.getenv("SEND_EMAILS_ENDPOINT")
        
    def send_email(self, to_email: str, subject: str, html_content: str) -> bool:
        """Send HTML email using Microsoft Graph API"""
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        email_body = {
            "message": {
                "subject": subject,
                "body": {
                    "contentType": "HTML",
                    "content": html_content
                },
                "toRecipients": [
                    {
                        "emailAddress": {
                            "address": to_email
                        }
                    }
                ]
            }
        }
        
        try:
            response = requests.post(
                self.graph_api_endpoint,
                headers=headers,
                json=email_body,
                timeout=30
            )
            
            if response.status_code == 202:
                logger.info(f"Email sent successfully to {to_email}")
                return True
            else:
                logger.error(f"Failed to send email. Status code: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending email: {str(e)}")
            return False

def handle_auto_response(email_data: Dict, access_token: str, user_prompt: str, preview_only: bool = False) -> Dict:
    """Main function to handle the complete auto-response flow"""
    
    try:
        response_generator = AutoResponseGenerator()
        
        if preview_only:
            generated_response = response_generator.generate_preview(email_data, user_prompt)
            if generated_response:
                return {
                    "status"       : "success",
                    "message"      : "Preview generated successfully",
                    "subject"      : generated_response["subject"],
                    "plain_text"   : generated_response["plain_text"],
                    "html_content" : generated_response["html_content"],
                    "preview"      : True
                }
        else:
            generated_response = response_generator.generate_response(email_data, user_prompt)
            
            if not generated_response:
                return {
                    "status": "error",
                    "message": "Failed to generate appropriate response"
                }
                
            email_sender = EmailSender(access_token)
            send_success = email_sender.send_email(
                to_email     = email_data['sender_email'],
                subject      = generated_response['subject'],
                html_content = generated_response['html_content']
            )
            
            if send_success:
                return {
                    "status"       : "success",
                    "message"      : "Response sent successfully",
                    "subject"      : generated_response["subject"],
                    "plain_text"   : generated_response["plain_text"],
                    "html_content" : generated_response["html_content"]
                }
        
        return {
            "status"  : "error",
            "message" : "Failed to generate or send response"
        }
            
    except Exception as e:
        logger.error(f"Error in auto-response handling: {str(e)}")
        
        return {
            "status"  : "error",
            "message" : f"Error processing auto-response: {str(e)}"
        }


@tool
def RespondToEmailBasedOnUserPrompt():
    """ Respond to an email based on the user's input, and provided email context (if any) """


def RespondToEmailNode(state: AgentState):
    """ Respond to an email based on the user's input, and provided email context (if any) """

    messages = state.get("messages", [])
    if not messages:
        logger.warning("AGENTS/RESPONSE_AGENT - RespondToEmailNode() - No messages in state")
        return state
    
    # Get latest AI message
    last_message = messages[-1]
    
    if not isinstance(last_message, AIMessage) or not last_message.tool_calls:
        logger.warning("AGENTS/RESPONSE_AGENT - RespondToEmailNode() - Last message is not an AIMessage or has no tool calls")
        return state
    
    tool_call = last_message.tool_calls[0]

    try:
        # This will throw an exception for key error
        dummy = state["email_context"]["email_id"]
        
        email_data = {
            "sender_email"      : state["email_context"].get("sender_email", None),
            "sender_name"       : state["email_context"].get("sender_name", None),
            "recipient_email"   : state["email_context"].get("recipient_email", None),
            "recipient_name"    : state["email_context"].get("recipient_name", None),
            "body"              : state["email_context"].get("body", None),
            "subject"           : state["email_context"].get("subject", None)
        }

    except Exception as exception:
        logger.error(f"AGENTS/RESPONSE_AGENT - RespondToEmailNode() - Email context in state is available")
        logger.error(f"AGENTS/RESPONSE_AGENT - RespondToEmailNode() - Email context in state is available {exception}")
        return state

    access_token = "eyJ0eXAiOiJKV1QiLCJub25jZSI6IktpaXlrNEJGYXVGa2JCUHdCY0FGMnpGN0JSLWl0ZkIybHVsV0tXQlN6TlkiLCJhbGciOiJSUzI1NiIsIng1dCI6Inp4ZWcyV09OcFRrd041R21lWWN1VGR0QzZKMCIsImtpZCI6Inp4ZWcyV09OcFRrd041R21lWWN1VGR0QzZKMCJ9.eyJhdWQiOiJodHRwczovL2dyYXBoLm1pY3Jvc29mdC5jb20iLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC9hOGVlYzI4MS1hYWEzLTRkYWUtYWM5Yi05YTM5OGI5MjE1ZTcvIiwiaWF0IjoxNzMzOTc5NDE1LCJuYmYiOjE3MzM5Nzk0MTUsImV4cCI6MTczMzk4NDgyMywiYWNjdCI6MCwiYWNyIjoiMSIsImFpbyI6IkFXUUFtLzhZQUFBQUZCeHRDNjByTERreXJGa3prYXIycTBjOVFTUmdnMmpmeHFpeEFjUUpmMzZUZ0NlMkRiWGlnRmZFODBBSlgrWWloczVBbjNEZXpGcHBGelhYekxEdmd6TUxFR09VVWV6NXZvd3o5M2IwYkR5eW9XNTNwNzl3U1NFb0JTRVZZbldXIiwiYW1yIjpbInB3ZCJdLCJhcHBfZGlzcGxheW5hbWUiOiJHcmFwaCBlbWFpbCB0ZXN0IiwiYXBwaWQiOiI5OGQ5N2E4NS02MzkyLTQ5NWUtODA1ZS1iNjU5YTE2NmQ1YjEiLCJhcHBpZGFjciI6IjEiLCJmYW1pbHlfbmFtZSI6Ik5hc2lrYSIsImdpdmVuX25hbWUiOiJEZWVwdGhpIiwiaWR0eXAiOiJ1c2VyIiwiaXBhZGRyIjoiMTU1LjMzLjEzMy4xNjYiLCJuYW1lIjoiRGVlcHRoaSBOYXNpa2EiLCJvaWQiOiJjMzdlNjNiYy1hMWE0LTQxMDgtYmVmMy05YmU0ZDY0NTVkYTEiLCJvbnByZW1fc2lkIjoiUy0xLTUtMjEtMTk0MzYyNjIzMi03MzQwODQzNS0xMjI2NDQyODgtMTM5MzY4OCIsInBsYXRmIjoiNSIsInB1aWQiOiIxMDAzMjAwMzAzOTUxMURDIiwicmgiOiIxLkFWa0FnY0x1cUtPcXJrMnNtNW81aTVJVjV3TUFBQUFBQUFBQXdBQUFBQUFBQUFCWkFDcFpBQS4iLCJzY3AiOiJGaWxlcy5SZWFkV3JpdGUuQWxsIE1haWwuUmVhZEJhc2ljIE1haWwuUmVhZFdyaXRlIE1haWwuU2VuZCBNYWlsYm94U2V0dGluZ3MuUmVhZCBvcGVuaWQgcHJvZmlsZSBTaXRlcy5SZWFkV3JpdGUuQWxsIFVzZXIuUmVhZCBVc2VyLlJlYWRCYXNpYy5BbGwgZW1haWwiLCJzaWduaW5fc3RhdGUiOlsiaW5rbm93bm50d2siLCJrbXNpIl0sInN1YiI6InB4UFdxWERVYlFHREQ4RzNBemlsTWx0bzZydFB4MmZwWnc3VHZubWZnS2siLCJ0ZW5hbnRfcmVnaW9uX3Njb3BlIjoiTkEiLCJ0aWQiOiJhOGVlYzI4MS1hYWEzLTRkYWUtYWM5Yi05YTM5OGI5MjE1ZTciLCJ1bmlxdWVfbmFtZSI6Im5hc2lrYS5kQG5vcnRoZWFzdGVybi5lZHUiLCJ1cG4iOiJuYXNpa2EuZEBub3J0aGVhc3Rlcm4uZWR1IiwidXRpIjoic1VXd2RINFVIMDZyRzJleWJIczBBQSIsInZlciI6IjEuMCIsIndpZHMiOlsiYjc5ZmJmNGQtM2VmOS00Njg5LTgxNDMtNzZiMTk0ZTg1NTA5Il0sInhtc19pZHJlbCI6IjEgMjIiLCJ4bXNfc3QiOnsic3ViIjoiQ3FSOWJmX2pDZFZNQ0NzWE9UMWRQODFhcjB6UmE4d0VQVXRZOHI3Y3FKMCJ9LCJ4bXNfdGNkdCI6MTM4OTAzMDQ3N30.LDbEmK_B_1z9se9EcpLMsVuV11TRmeGDz0Om2NSBIcziNAGtU3DHcvemf2GtPplrGzkwd76TWPPutTabZeI2F9wvsTnsslvUcx50Cuxduk_vVf77Wkhni45tgLp0eJ71howre4vAo8MJ07--YuEbMiOkbzDHB0Z9BPBzfQ0oyx1pC5r6LwqCsPtVxHsf-PrIHlsFQ-BkDsOrrGckvW692LlCLmzBi-3vh7P-GZ6DnQfFKl0kDQdBw4tMSqGHaJ-yMHtZLu2fcPk8yUN7QMC6Ilud26NGhOEyFHMdQkuv5TzTr13WbrDjzGZ5BSFv16GSvNkv47WMis-T6H7zF-peNg"

    if state.get('current_input', None):
        user_prompt = state['current_input']
    
    else:
        user_prompt = "For the given email, generate an appropriate response."

    result = handle_auto_response(email_data, access_token, user_prompt, preview_only=True)
    
    if result["status"] == "success":

        success_message  = ToolMessage(
            tool_call_id = tool_call.get("id"),
            content      = f"Successfully generated response via Response agent"
        )

        state["messages"].append(success_message)
        state["response_output"] = {
            "sender_email"   : state["email_context"].get("sender_email", None),
            "sender_name"    : state["email_context"].get("sender_name", None),
            "subject"           : result["subject"],
            "body"              : result["html_content"],
            "plain_text"        : result["plain_text"],
        }

    else:
        error_message    = ToolMessage(
            tool_call_id = tool_call.get("id"),
            content      = "An exception occurred while generating a response via Response Agent"
        )

        state["messages"].append(error_message)
    
    return state
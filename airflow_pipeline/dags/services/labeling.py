import os
import re
import json
import requests
from dotenv import load_dotenv
from services.logger import start_logger

# Load env
load_dotenv()

# Start logging
logger = start_logger()

def replace_urls(text):
    ''' Replace URLs with placeholders '''
    logger.info(f"Airflow - services/labeling.py - replace_urls() - Removing URLs from email body")
    
    # Regex pattern for detecting URLs
    url_pattern = r"https?://\S+|www\.\S+"

    return re.sub(pattern=url_pattern, repl="URL", string=text)

def filter_response(response):
    ''' Parse the response from the language model to extract categories '''

    # Even though we provide a prompt to the language model 
    # clearly stating the requirements and output format, 
    # the model may sometimes ignore everything and produce a response.

    # This function ensures that only the required categories
    # from the response are assigned as categories.

    logger.info(f"Airflow - services/labeling.py - filter_response() - Filtering language model's response...")

    if not response:
        logger.error(f"Airflow - services/labeling.py - filter_response() - Filtering language model's response...")
        return None

    lines = response.strip().splitlines()
    
    if not lines:
        logger.error(f"Airflow - services/labeling.py - filter_response() - No lines were found")
        return None

    # Get the first line as it should contain the categories
    first_line = lines[0].strip()

    # Check if the first line contains comma-separated values
    # If not, assume the first line is a single category
    if "," in first_line:
        categories = [category.strip().title() for category in first_line.split(",") if category.strip()]
    else:
        categories = [first_line[:10].title()] 

    if categories:
             
        # Check if categories contain valid entries
        if (len(categories) < 4) or (len(categories) == 1 and categories[0].lower() not in {"error", ""}):
            logger.info(f"Airflow - services/labeling.py - filter_response() - Categories assigned: {categories}")
            return categories
        
        else:
            # Handle when a long irrelevant text might be provided
            logger.warning(f"Airflow - services/labeling.py - filter_response() - Long response received (See full response below)")
            logger.warning(f"Airflow - services/labeling.py - filter_response() - Response: {categories[:5]}")
            return ["ERROR"] + categories[:5]
    
    else:
        logger.error(f"Airflow - services/labeling.py - filter_response() - No categories returned. Assigning 'ERROR'")
        return ["ERROR"]


def label_email(email_dict: dict):
    ''' Categorize each email by passing them to a locally available Language Model '''

    # For our usecase, we will be running Microsoft Phi-3 128k-instruct
    # language model locally via Ollama. Ensure Ollama server is running.
    
    logger.info(f"Airflow - services/labeling.py - label_email() - Categorizing email...")
    
    labels = []
    reply_to_addresses = ""
    reply_to_list = None
    
    email_addresses = []
    email_dict["body"] = replace_urls(email_dict["body"])

    try:
        if email_dict['reply_to']:
            reply_to_list = json.loads(str(email_dict['reply_to']))[0]

            # Loop through the list and parse each emailAddress field
            if len(reply_to_list) > 1:
                
                for item in reply_to_list:
                    for key, val in item.items():
                        
                        contents = json.loads(str(val).replace("'", '"'))
                        email_addresses.append(contents["address"])
            else:
                for key, val in reply_to_list.items():
                    
                    contents = json.loads(str(val).replace("'", '"'))
                    email_addresses.append(contents["address"])
    
    except Exception as exception:
        logger.error(f"Airflow - services/labeling.py - label_email() - An exception occurred when parsing reply_to emails (See exception below)")
        logger.error(f"Airflow - services/labeling.py - label_email() - {exception}")


    if email_addresses:
        reply_to_addresses = ", ".join(email_addresses)
    
    prompt = f"""      
        Your task is to assign specific categories to emails based on their content. 

        Here are the contents of an email:
        Sender: {email_dict["sender_email"]}
        Subject: {email_dict["subject"]}
        Body: {email_dict["body"]}
        Reply To: {reply_to_addresses}
        
        The available categories are: 
        WORK
        MARKETING
        SOCIAL
        UPDATES
        PERSONAL
        BILLING
        TRAVEL
        EDUCATION
        HEALTH
        PROFANITY
        SPAM 
        OTHER (Emails that do not fit into any of the above categories. EMAILS BELONGING TO 'OTHER' CATEGORY CANNOT BELONG TO ANY OTHER CATEGORY.)

        Task:
        For the above email I provided, what three categories would you assign to the email? Only provide the names of the categories.

        Example Output:
        Marketing, Social

        RESTRICTION: YOUR OUTPUT CANNOT EXCEED THREE WORDS. THE CATEGORIES YOU PROVIDE MUST BE PRESENT IN THE LIST OF AVAILABLE CATEGORIES PROVIDED.
    """
    
    try:
        headers = {
            "Content-Type": "application/json",
        }
        
        logger.info(f"Airflow - services/labeling.py - label_email() - Sending prompt and email contents to language model...")

        response = requests.post(
            url     = "http://" + os.getenv("OLLAMA_HOST") + ":" + os.getenv("OLLAMA_PORT") + os.getenv("OLLAMA_ENDPOINT"),
            json    = {
                "model"  : os.getenv("OLLAMA_MODEL"), 
                "prompt" : prompt,
                "stream" : False,
                
                # Changing the below parameters will severely affect the model's
                # performance. Change only if you know what you are doing.
                
                "options": {
                    "temperature"   : 0,
                    "top_k"         : 1,
                    "top_p"         : 0.1,
                    "mirostat_tau"  : 0.0,
                    "num_ctx"       : 10000
                }
            },
            headers = headers,
        )
        
        if response.status_code == 200:
            logger.info(f"Airflow - services/labeling.py - label_email() - Response received successfully from language model")

            response_json = response.json()
            category = response_json.get("response", "").strip()
            
            if category:
                labels = filter_response(response=category)
            else:
                logger.error(f"Airflow - services/labeling.py - label_email() - Invalid response received from the language model (See content below)")
                logger.error(f"Airflow - services/labeling.py - label_email() - {category}")
        
        else:
            raise Exception(f"Something went wrong while connecting to language model. Status code: {response.status_code}, Message: {response.text}") 
    
    except Exception as exception:
        logger.error(f"Airflow - services/labeling.py - label_email() - An exception occurred (See exception below)")
        logger.error(f"Airflow - services/labeling.py - label_email() - {exception}")

    finally:
        return labels

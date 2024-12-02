# variables.py 
# Load all environment variables 

import os
from typing import Dict, Optional
from dotenv import dotenv_values

# Optionally, if you want load_dotenv as usual
# from dotenv import load_dotenv

def load_env_vars() -> Optional[Dict[str, str]]:
    ''' Load all environment variables into an global object '''

    try:
        # Load the env
        if not dotenv_values():
            raise

        # Optionally, if you want load_dotenv as usual
        # load_dotenv() 
        # env = dict(os.environ)
        
        env = dotenv_values()
        return env
        
    except Exception as exception:
        print("ERROR: .env file is either missing or the provided path is incorrect. The application will now kill itself... Bye!")
        exit(1)
    
    # If this part ever gets executed
    return None

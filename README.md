## FastAPI
---
### Prerequisites
- If you do not have `CLIENT_ID` and `CLIENT_SECRET`, please get them by creating and registering your application with Microsoft Azure Active Directory
- Here's a step-by-step article that will help you setup Active Directory: [Walkthrough Article](https://medium.com/@megan.baye/access-microsoft-outlook-mail-using-python-through-the-microsoft-graph-api-4b8d11a0aae5)
---
### Setup instructions
1. Clone this repo:
    ```bash
    git clone -b https://github.com/BigDataIA-Fall2024-TeamB6/FinalProject.git
    ```
    and navigate to it in your terminal:
    ```bash
    cd FinalProject/fastapi
    ```
2. Create a conda environment with Python 3.12
    ```bash
    conda create -n fastapi_env python==3.12
    ```
3. Activate the environment
    ```bash
    conda activate fastapi_env
    ```
4. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
5. Create a `.env` file and copy-paste the contents of `.env.example` to `.env`
6. Add your `CLIENT_ID` and `CLIENT_SECRET` to the `.env` file
7. Start the FastAPI server (Uvicorn):
    ```bash
    python app.py
    ```
8. View the endpoints in your web browser by visting 
    ```bash
    http://localhost:5000/docs
    ```
    or
    ```bash
    http://127.0.0.1:5000/docs #Recommended for Mac users
    ``` 
9. Most of the configurable aspects of the application are in the `.env` file. Update the file accordingly *(incorrect modifications will break the application)*.
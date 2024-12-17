import uvicorn
from routes import auth, extras
from fastapi import FastAPI
from utils.variables import load_env_vars
from fastapi.middleware.cors import CORSMiddleware

# Check if the env file is present before loading the application
env = load_env_vars()

# Initialize the app
app = FastAPI(
    debug = env["APP_DEBUG"],
    title = env["APP_TITLE"]
)

# Allow CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins     = ["*"],
    allow_methods     = ["*"],
    allow_headers     = ["*"],
    allow_credentials = True
)

# Attach routes to the application
app.include_router(router=extras.router)
app.include_router(router=auth.router)

# Run the server manually with Uvicorn
def main():
    uvicorn.run(
        app  = app,
        host = env["HOST_ADDRESS"],
        port = int(env["HOST_PORT"])
    )

if __name__ == "__main__":
    main()
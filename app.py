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
app.include_router(extras.router)
app.include_router(auth.router)

# Run the server manually with Uvicorn
if __name__ == "__main__":
    uvicorn.run(
        app  = app,
        host = env["HOST_ADDRESS"],
        port = int(env["HOST_PORT"])
    )
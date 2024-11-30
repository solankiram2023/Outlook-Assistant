from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from utils.logs import start_logger

# Start the router
router  = APIRouter()

@router.get(
    path        = "/health",
    name        = "Health",
    description = "Route to check if FastAPI is running",
    tags        = ["Core"]
)
def healthcheck():

    # Logging
    logger = start_logger()
    logger.info("ROUTES/EXTRAS - healthcheck() - GET /health request received")

    return JSONResponse(
        status_code = status.HTTP_200_OK,
        content     = {
            "status"    : status.HTTP_200_OK,
            "type"      : "string",
            "message"   : "FastAPI is up and running!"
        }
    )
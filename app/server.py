import json
from app.api import router
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from app.utils.database import SessionLocal
from app.utils.common import CustomException

from . import __title__, __version__, __description__

api = FastAPI(
    title=__title__,
    description=__description__,
    version=__version__,
)

api.include_router(router)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@api.exception_handler(CustomException)
async def custom_exception_handler(request: Request, exc: CustomException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "message": exc.message},
    )


with open("openapi.json", "w") as f:
    f.write(json.dumps(api.openapi(), indent=4))

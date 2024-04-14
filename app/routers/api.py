from fastapi import APIRouter

from app.settings.settings import API_VERSION
from app.routers import db

router = APIRouter(
    prefix=f'/api/v{API_VERSION}',
    responses={404: {"description": "Not found"}},
)


router.include_router(db.router)

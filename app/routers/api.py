from fastapi import APIRouter

from app.routers.settings import API_VERSION

from app.routers import shard

router = APIRouter(
    prefix=f'/api/v{API_VERSION}',
    responses={ 404: { "description": "Not Found" } },
)

router.include_router(shard.router)
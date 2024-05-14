from fastapi import FastAPI

from app.db.db import Database
from app.routers import api
from logging.config import dictConfig
import logging
from app.settings.config import LogConfig

dictConfig(LogConfig().dict())
logger = logging.getLogger("database")

app = FastAPI(debug=True)
app.include_router(api.router)


@app.get("/")
async def root():
    return {"message": "Usage: /api/v1/db/command/args"}


@app.on_event("startup")
async def startup_event():
    logger.info('Starting up')
    Database()


@app.on_event("shutdown")
async def shutdown_event():
    logger.info('Shutting down')
    Database().dump_to_fs()

import logging
from typing import Annotated

from fastapi import FastAPI, Path

from app.db.db import Database
from app.routers import api
from logging.config import dictConfig
from app.config import LogConfig

dictConfig(LogConfig().dict())

logger = logging.getLogger(__name__)
app = FastAPI(debug=True)
app.include_router(api.router)


@app.get("/")
async def root():
    return {"message": "Usage: /api/v1/db/command/args"}


@app.on_event("startup")
async def startup_event():
    logger.info('Starting up')


@app.on_event("shutdown")
async def shutdown_event():
    logger.info('Shutting down')
    db = Database()
    db.dump_to_fs()

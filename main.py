import logging
from fastapi import FastAPI

from db.db import Database

from logging.config import dictConfig
from config import LogConfig

dictConfig(LogConfig().dict())

logger = logging.getLogger(__name__)
app = FastAPI(debug=True)
db = Database()


@app.get("/")
async def root():
    return {"message": "Usage: /api/"}


# @app.post("/api/{commanPicklingErrord}")
# async def say_hello(name: str):
#     return


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}


@app.on_event("startup")
async def startup_event():
    logger.info('Starting up')
    print('started')


@app.on_event("shutdown")
async def shutdown_event():
    logger.info('Shutting down')
    db.dump_to_fs()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)

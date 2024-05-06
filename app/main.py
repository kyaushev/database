from fastapi import FastAPI
from app.routers import api

app = FastAPI(debug=True)
app.include_router(api.router)

@app.get("/")
async def root():
    return { "message": "route" }

@app.on_event("startup")
async def sturtup_event():
    print("started")

@app.on_event("shutdown")
async def shutdown_event():
    print("shutting down")
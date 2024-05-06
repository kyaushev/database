from fastapi import APIRouter, HTTPException, Path
from typing import Annotated
import requests
import hashlib
import json

from app.models.transactions import TransactionType
from app.cache.cache import ConfigCache
from app.models import command

router = APIRouter(
    prefix="/shard",
    tags=["shard"],
    responses={ 404: { "description": "Not Found" } }
)

@router.post("/begin/{transaction_type}")
async def begin(transaction_type: Annotated[TransactionType, Path(title="Transaction type")]) -> dict:
    config = ConfigCache()
    for shard in config.shards:
        response = requests.post(shard["url"] + f"/api/v1/db/begin/{transaction_type.value}")
        if response.status_code == 400:
            raise HTTPException(status_code=400, detail=response.reason)
    body = response.json()
    return body

@router.post("/commit/{transaction_id}")
async def commit(transaction_id: Annotated[int, Path(title="Transaction ID to commit")]) -> dict:
    config = ConfigCache()
    for shard in config.shards:
        response = requests.post(shard["url"] + f"/api/v1/db/commit/{transaction_id}")
    body = response.json()
    return body

@router.post("/insert_one/{transaction_id}")
async def insert_one(transaction_id: int, cmd: command.Insert) -> dict:
    hashed_key: str = hashlib.md5(cmd.name.encode()).hexdigest()
    first: str = hashed_key[0]
    config = ConfigCache()
    url = ""
    for shard in config.shards:
        if first <= shard["max_hashed_key"]:
            url = shard["url"]
            break
    response = requests.post(url + f"/api/v1/db/insert_one/{transaction_id}", json=cmd.__dict__)
    body = response.json()
    return body

@router.post("/insert_many/{transaction_id}")
async def insert_many(transaction_id: int, cmds: list[command.Insert]) -> set[dict]:
    list_cmds: list[list[command.Insert]] = []
    config = ConfigCache()
    ranges = sorted(map(lambda x: x["max_hashed_key"], config.shards))
    response = set()
    for shard in config.shards:
        if shard["_id"] == 0:
            list_cmds[shard["_id"]] = list(filter(lambda x: hashlib.md5(x.name.encode()).hexdigest()[0] <= ranges[shard["_id"]], cmds))
        else:
            list_cmds[shard["_id"]] = list(filter(lambda x: ranges[shard["_id"] - 1] < hashlib.md5(x.name.encode()).hexdigest()[0] <= ranges[shard["_id"]], cmds))
    for shard in config.shards:
        if (len(list_cmds[shard["_id"]]) > 0):
            response = response | set(requests.post(shard["url"] + f"/api/v1/db/insert_many/{transaction_id}", json=json.dumps([cmd.__dict__ for cmd in list_cmds[shard["_id"]]])).json())
    return response

@router.post("/find/{transaction_id}")
async def find(transaction_id: int, cmd: command.Find) -> dict:
    config = ConfigCache()
    result = []
    for shard in config.shards:
        response = requests.post(shard["url"] + f"api/v1/db/find/{transaction_id}", json=cmd.__dict__)
        found = response.json()
        result += found
    result = sorted(result, key=result["id"])
    if cmd.filter.limit is None or cmd.filter.limit == -1:
        return result
    result = result[:cmd.filter.limit]
    return result

@router.post("/update/{transaction_id}")
async def update(transaction_id: int, cmd: command.Update) -> dict:
    config = ConfigCache()
    response = set()
    for shard in config.shards:
        response = response | set(requests.post(shard["url"] + f"/update/{transaction_id}", json=cmd.__dict__))
    return response

@router.post("/delete/{transaction_id}")
async def update(transaction_id: int, cmd: command.Delete) -> dict:
    config = ConfigCache()
    response = set()
    for shard in config.shards:
        response = response | set(requests.post(shard["url"] + f"/update/{transaction_id}", json=cmd.__dict__))
    return response
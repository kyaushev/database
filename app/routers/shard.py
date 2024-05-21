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
        replica_urls = shard["replicas"]
        for replica_url in replica_urls:
            response = requests.post(replica_url + f"/api/v1/db/begin/{transaction_type.value}")
            if response.status_code == 400:
                raise HTTPException(status_code=400, detail=response.reason)
    body = response.json()
    return body

@router.post("/commit/{transaction_id}")
async def commit(transaction_id: Annotated[int, Path(title="Transaction ID to commit")]) -> dict:
    config = ConfigCache()
    for shard in config.shards:
        replica_urls = shard["replicas"]
        for replica_url in replica_urls:
            response = requests.post(replica_url + f"/api/v1/db/commit/{transaction_id}")
    body = response.json()
    return body

@router.post("/insert_one/{transaction_id}")
async def insert_one(transaction_id: int, cmd: command.Insert) -> dict:
    hashed_key: str = hashlib.md5(cmd.name.encode()).hexdigest()
    first: str = hashed_key[0]
    config = ConfigCache()
    for shard in config.shards:
        if first <= shard["max_hashed_key"]:
            replica_urls = shard["replicas"]
            for replica_url in replica_urls:
                response = requests.post(replica_url + f"/api/v1/db/insert_one/{transaction_id}", json=cmd.__dict__)
                body = response.json()
            return body

@router.post("/insert_many/{transaction_id}")
async def insert_many(transaction_id: int, cmds: list[command.Insert]) -> set[dict]:
    for cmd in cmds:
        hashed_key: str = hashlib.md5(cmd.name.encode()).hexdigest()
        first: str = hashed_key[0]
        config = ConfigCache()
        for shard in config.shards:
            if first <= shard["max_hashed_key"]:
                replica_urls = shard["replicas"]
                for replica_url in replica_urls:
                    response = requests.post(replica_url + f"/api/v1/db/insert_one/{transaction_id}", json=cmd.__dict__)
                    body = response.json()
    return body

@router.post("/find/{transaction_id}")
async def find(transaction_id: int, cmd: command.Find) -> dict:
    config = ConfigCache()
    result = []
    for shard in config.shards:
        replica_urls = shard["replicas"]
        shard_url = replica_urls[0]
        response = requests.post(shard_url + f"api/v1/db/find/{transaction_id}", json=cmd.__dict__)
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
        replica_urls = shard["replicas"]
        for replica_url in replica_urls:
            response = response | set(requests.post(replica_url + f"/update/{transaction_id}", json=cmd.__dict__))
    return response

@router.post("/delete/{transaction_id}")
async def delete(transaction_id: int, cmd: command.Delete) -> dict:
    config = ConfigCache()
    response = set()
    for shard in config.shards:
        replica_urls = shard["replicas"]
        for replica_url in replica_urls:
            response = response | set(requests.post(replica_url + f"/delete/{transaction_id}", json=cmd.__dict__))
    return response
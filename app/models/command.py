from typing import Literal, Dict, Any

from pydantic import BaseModel


class Cond(BaseModel):
    field: str
    predicate: Literal["$eq", "$ne", "$lt", "$lte", "$gt", "$gte"]
    value: str


class Filter(BaseModel):
    cond: Cond
    limit: int | None = -1


class Insert(BaseModel):
    name: str
    doc: dict


class Find(BaseModel):
    filter: Filter


class Update(BaseModel):
    filter: Filter
    set: dict


class Delete(BaseModel):
    filter: Filter

class Record(BaseModel):
    _id: int
    name: str
    doc: Dict[str, Any]  # Словарь с произвольными ключами и значениями
    created_id: int
    expired_id: int

class Oplog(BaseModel):
    action: str
    record: Record
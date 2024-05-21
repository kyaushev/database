from typing import Literal

from pydantic import BaseModel


class Cond(BaseModel):
    field: str | None = None
    predicate: Literal["$eq", "$ne", "$lt", "$lte", "$gt", "$gte"] | None = None
    value: str | None = None


class Filter(BaseModel):
    cond: Cond | None = Cond()
    limit: int | None = -1


class Insert(BaseModel):
    name: str
    doc: dict


class Find(BaseModel):
    filter: Filter | None = Filter()


class Update(BaseModel):
    filter: Filter | None = Filter()
    set: dict


class Delete(BaseModel):
    filter: Filter | None = Filter()

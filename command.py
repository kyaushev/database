from typing import Literal

from pydantic import BaseModel

#
# class Command(BaseModel):
#     type: Literal['ReadUncommitted', 'ReadCommittedTransaction', 'RepeatableRead', 'Serializable']
#     actions: str | None = None
#
# class Action(BaseModel):
#     type

from typing import Annotated

from fastapi import APIRouter, HTTPException, Path

from app.db.db import Database
from app.db.transaction import TransactionType
from app.models import command

router = APIRouter(
    prefix="/db",
    tags=["database"],
    responses={404: {"description": "Unexpected command found"}}
)


# Path like '/api/v1/db/{command}[/{args}]'
@router.post("/begin/{transaction_type}")
async def begin(transaction_type: Annotated[TransactionType, Path(title="Transaction type")]) -> dict:
    try:
        db = Database()
        match transaction_type:
            case TransactionType.read_uncommitted:
                t_id = db.begin_read_uncommitted_transaction()
            case TransactionType.read_committed:
                t_id = db.begin_read_committed_transaction()
            case TransactionType.repeatable_read:
                t_id = db.begin_repeatable_read_transaction()
            case TransactionType.serializable:
                t_id = db.begin_serializable_transaction()
            case _:
                t_id = db.begin_transaction()
    except Exception as e:
        # logger.
        raise HTTPException(status_code=400, detail=e)

    return {"transaction_id": t_id}


@router.post("/commit/{transaction_id}")
async def commit(transaction_id: Annotated[int, Path(title="Transaction ID to commit")]) -> dict:
    db = Database()
    db.commit(transaction_id)
    msg = {"message": "Success"}
    return msg


@router.post("/insert_one/{transaction_id}")
async def insert_one(transaction_id: int, cmd: command.Insert) -> dict:
    db = Database()
    transaction = db.transactions[transaction_id]
    transaction.add_record(name=cmd.name, doc=cmd.doc)
    msg = {"message": "Success"}
    return msg


@router.post("/insert_many/{transaction_id}")
async def insert_many(transaction_id: int, cmds: list[command.Insert]) -> set[dict]:
    res = [await insert_one(transaction_id, cmd) for cmd in cmds]
    return set(res)


@router.post("/find/{transaction_id}")
async def find(transaction_id: int, cmd: command.Find) -> dict:
    return {}


@router.post("/update/{transaction_id}")
async def update(transaction_id: int, cmd: command.Find) -> dict:
    db = Database()
    transaction = db.transactions[transaction_id]
    transaction.update_record("name", {})
    msg = {"message": "Success"}
    return msg


@router.post("/delete/{transaction_id}")
async def update(transaction_id: int, cmd: command.Delete) -> dict:
    db = Database()
    transaction = db.transactions[transaction_id]
    transaction.delete_record("name")
    msg = {"message": "Success"}
    return msg


@router.post("/replicate")
async def begin(oplog: list[command.Oplog]) -> dict:
    try:
        db = Database()
        t_id = db.begin_transaction()
        transaction = db.transactions[t_id]
        for op in oplog:
            record = op['record']
            match op['action']:
                case 'insert':
                    transaction.add_record(
                        name=record['name'],
                        doc=record['name'],
                        id=record['id'],
                        created_id=record['created_id'],
                        expired_id=record['expired_id']
                    )
                case 'delete':
                    transaction.delete_record(record['name'])
        db.commit(t_id, replicate=False)
    except Exception as e:
        # logger.
        raise HTTPException(status_code=400, detail=e)

    return {"transaction_id": t_id}

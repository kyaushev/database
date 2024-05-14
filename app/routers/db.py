import logging
from typing import Annotated

from fastapi import APIRouter, HTTPException, Path

from app.db.db import Database
from app.db.transaction import TransactionType
from app.models import command

logger = logging.getLogger("database")

router = APIRouter(
    prefix="/db",
    tags=["database"],
    responses={404: {"description": "Unexpected command found"}}
)


@router.post("/begin/{transaction_type}")
async def begin(transaction_type: TransactionType) -> dict:
    try:
        t_id = Database().begin_transaction(transaction_type)
        logger.info(f"/begin/{transaction_type}/{t_id}")
    except Exception as e:
        logger.critical(f'Begin error in transaction: {transaction_type}', exc_info=True)
        raise HTTPException(status_code=400, detail=e)

    return {"transaction_id": t_id}


@router.post("/commit/{transaction_id}", status_code=201)
async def commit(transaction_id: Annotated[int, Path(title="Transaction ID to commit")]) -> dict:
    try:
        Database().commit(transaction_id)
        logger.info(f"/commit/{transaction_id}/{transaction_id}")
    except Exception as e:
        logger.critical(f'Commit error in transaction: {transaction_id}', exc_info=True)
        raise HTTPException(status_code=400, detail=e)

    return {"message": "Success"}


@router.post("/insert_one/{transaction_id}")
async def insert_one(transaction_id: int, cmd: command.Insert) -> dict:
    try:
        Database().insert(transaction_id=transaction_id, name=cmd.name, doc=cmd.doc)
        logger.info(f"/insert_one/{transaction_id}\n{cmd}")
    except Exception as e:
        logger.critical(f'Insert error in transaction: {transaction_id}', exc_info=True)
        raise HTTPException(status_code=400, detail=e)
    return {"message": "Success"}


@router.post("/insert_many/{transaction_id}")
async def insert_many(transaction_id: int, cmds: list[command.Insert]) -> set[dict]:
    res = [await insert_one(transaction_id, cmd) for cmd in cmds]
    return set(res)


@router.post("/find/{transaction_id}")
async def find(transaction_id: int, cmd: command.Find) -> list[dict]:
    try:
        if transaction_id in Database().transactions:
            values = Database().find(
                transaction_id=transaction_id,
                limit=cmd.filter.limit,
                field=cmd.filter.cond.field,
                p=cmd.filter.cond.predicate,
                value=cmd.filter.cond.value
            )
            logger.info(f"/find/{transaction_id}\n{cmd}")
        else:
            logger.critical(f'Transaction: {transaction_id} not found', exc_info=True)
            raise HTTPException(status_code=400, detail=f'Transaction: {transaction_id} not found')
    except Exception as e:
        logger.critical(f'Find error in transaction: {transaction_id}', exc_info=True)
        raise HTTPException(status_code=400, detail=e)
    return values


@router.post("/update/{transaction_id}")
async def update(transaction_id: int, cmd: command.Update) -> dict:
    try:
        if transaction_id in Database().transactions:
            Database().update(
                transaction_id=transaction_id,
                limit=cmd.filter.limit,
                field=cmd.filter.cond.field,
                p=cmd.filter.cond.predicate,
                value=cmd.filter.cond.value,
                new_doc=cmd.set
            )
            logger.info(f"/update/{transaction_id}\n{cmd}")
        else:
            logger.critical(f'Transaction: {transaction_id} not found', exc_info=True)
            raise HTTPException(status_code=400, detail=f'Transaction: {transaction_id} not found')
    except Exception as e:
        logger.critical(f'Update error in transaction: {transaction_id}', exc_info=True)
        raise HTTPException(status_code=400, detail=e)
    return {"message": "Success"}


@router.post("/delete/{transaction_id}")
async def delete(transaction_id: int, cmd: command.Delete) -> dict:
    try:
        if transaction_id in Database().transactions:
            Database().delete(
                transaction_id=transaction_id,
                limit=cmd.filter.limit,
                field=cmd.filter.cond.field,
                p=cmd.filter.cond.predicate,
                value=cmd.filter.cond.value
            )
            logger.info(f"/delete/{transaction_id}\n{cmd}")
        else:
            logger.critical(f'Transaction: {transaction_id} not found', exc_info=True)
            raise HTTPException(status_code=400, detail=f'Transaction: {transaction_id} not found')
    except Exception as e:
        logger.critical(f'Delete error in transaction: {transaction_id}', exc_info=True)
        raise HTTPException(status_code=400, detail=e)
    return {"message": "Success"}

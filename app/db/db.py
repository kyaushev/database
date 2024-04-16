import logging
import pickle
import threading
import time
from typing import Dict

from app.db.collection import Collection
from app.db.transaction import (ReadUncommittedTransaction,
                                ReadCommittedTransaction,
                                RepeatableReadTransaction,
                                SerializableTransaction, Transaction, TransactionType)
from app.utils.meta_singleton import MetaSingleton

logger = logging.getLogger("database")


def predicate(p: str):
    match p:
        case "$eq":
            return lambda field, value: field == value
        case "$ne":
            return lambda field, value: field != value
        case "$lt":
            return lambda field, value: field < value
        case "$lte":
            return lambda field, value: field <= value
        case "$gt":
            return lambda field, value: field > value
        case "$gte":
            return lambda field, value: field >= value
        case _:
            return lambda field, value: False


class Database(metaclass=MetaSingleton):
    FILENAME = 'data/db.pickle'

    def __init__(self) -> None:
        self.collection: Collection = Collection()
        self.load_from_fs()
        self.transactions: Dict[int, Transaction] = {}
        thread = threading.Thread(target=self.dump_daemon, daemon=True)
        thread.start()
        logger.info(f'Database inited')

    @staticmethod
    def read() -> list:
        try:
            with open(Database.FILENAME, "rb") as f:
                data = pickle.load(f)
        except pickle.UnpicklingError:
            logger.critical('UnpicklingError', exc_info=True)
            data = []
        except FileNotFoundError:
            logger.warning('Database files not found!')
            data = []
        return data

    @staticmethod
    def write(data: list) -> None:
        try:
            with open(Database.FILENAME, "wb") as f:
                pickle.dump(data, f)
        except pickle.PicklingError:
            logger.critical('PicklingError', exc_info=True)

    @staticmethod
    def delete_transaction_id(snapshot):
        for record in snapshot:
            record['created_id'] = 0
        return snapshot

    def dump(self):
        snapshot = list.copy(self.collection.records)
        snapshot = Database.delete_transaction_id(snapshot)
        return snapshot

    def dump_to_fs(self) -> None:
        logger.info(f'Dumping to filesystem')
        Database.write(self.dump())

    def dump_daemon(self):
        while True:
            time.sleep(1*60)
            self.dump_to_fs()
            break

    def load_from_fs(self):
        self.collection.records = Database.read()

    def begin_transaction(self, transaction_type: str) -> int:
        match transaction_type:
            case TransactionType.read_uncommitted:
                t = self.collection.begin(ReadUncommittedTransaction)
            case TransactionType.read_committed:
                t = self.collection.begin(ReadCommittedTransaction)
            case TransactionType.repeatable_read:
                t = self.collection.begin(RepeatableReadTransaction)
            case TransactionType.serializable:
                t = self.collection.begin(SerializableTransaction)
            case _:
                t = self.collection.begin(ReadUncommittedTransaction)

        self.transactions[t.id] = t

        return t.id

    def commit(self, transaction_id: int) -> None:
        t: Transaction = self.transactions[transaction_id]
        t.commit()

    def insert(self, transaction_id: int, name: str, doc: dict) -> None:
        transaction = self.transactions[transaction_id]
        transaction.add_record(name=name, doc=doc)

    def find(self, transaction_id: int, limit: int, field: str, p: str, value: str) -> list[dict]:
        f = predicate(p)
        t: Transaction = self.transactions[transaction_id]
        target = [rec for rec in t.collection.records if f(str(rec.doc[field]), value)][limit]
        limit = len(target) if limit < 0 else max(len(target), limit)
        return target[:limit]

    def update(self, transaction_id: int, limit: int, field: str, p: str, value: str, new_doc: dict) -> None:
        t: Transaction = self.transactions[transaction_id]
        target = self.find(
            transaction_id=transaction_id,
            limit=limit,
            field=field,
            p=p,
            value=value
        )
        for record in target:
            t.update_record(_id=record["_id"], name=record["name"], doc=new_doc)

    def delete(self, transaction_id: int, limit: int, field: str, p: str, value: str):
        t: Transaction = self.transactions[transaction_id]
        target = self.find(
            transaction_id=transaction_id,
            limit=limit,
            field=field,
            p=p,
            value=value
        )
        for record in target:
            t.delete_record_id(_id=record["_id"])

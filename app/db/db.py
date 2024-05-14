import logging
import pickle
import threading
import time
from typing import Dict

from app.db.collection import Collection
from app.db.transaction import (ReadUncommittedTransaction,
                                ReadCommittedTransaction,
                                RepeatableReadTransaction,
                                SerializableTransaction, Transaction)
from app.utils.meta_singleton import MetaSingleton
from app.cache.cache import ConfigCache

logger = logging.getLogger(__name__)


class Database(metaclass=MetaSingleton):
    FILENAME = 'data/db.pickle'

    def __init__(self):
        self.collection: Collection = Collection()
        self.load_from_fs()
        self.transactions: Dict[int, Transaction] = {}
        thread = threading.Thread(target=self.dump_daemon, daemon=True)
        thread.start()
        logger.info(f'Database inited')

    @staticmethod
    def read():
        try:
            with open(Database.FILENAME, "rb") as f:
                data = pickle.load(f)
        except pickle.UnpicklingError:
            logging.critical('UnpicklingError', exc_info=True)
            data = []
        except FileNotFoundError:
            logging.warning('Database files not found!')
            data = []
        return data

    @staticmethod
    def write(data):
        try:
            with open(Database.FILENAME, "wb") as f:
                pickle.dump(data, f)
        except pickle.PicklingError:
            logging.critical('PicklingError', exc_info=True)

    @staticmethod
    def delete_transaction_id(snapshot):
        for record in snapshot:
            record['created_id'] = 0
        return snapshot

    def dump(self):
        snapshot = list.copy(self.collection.records)
        snapshot = Database.delete_transaction_id(snapshot)
        return snapshot

    def dump_to_fs(self):
        logger.info(f'Dumping to filesystem')
        Database.write(self.dump())

    def dump_daemon(self):
        while True:
            time.sleep(1*60)
            # self.dump_to_fs()
            break

    def load_from_fs(self):
        self.collection.records = Database.read()

    def begin_transaction(self):
        t: Transaction = self.begin_read_uncommitted_transaction()
        self.transactions[t.id] = t
        return t.id

    def begin_read_uncommitted_transaction(self):
        t: Transaction = self.collection.begin(ReadUncommittedTransaction)
        self.transactions[t.id] = t
        return t.id

    def begin_read_committed_transaction(self):
        t: Transaction = self.collection.begin(ReadCommittedTransaction)
        self.transactions[t.id] = t
        return t.id

    def begin_repeatable_read_transaction(self):
        t: Transaction = self.collection.begin(RepeatableReadTransaction)
        self.transactions[t.id] = t
        return t.id

    def begin_serializable_transaction(self):
        t: Transaction = self.collection.begin(SerializableTransaction)
        self.transactions[t.id] = t
        return t.id

    def send_oplog_entries(self, transaction_id):
        transaction = self.transactions[transaction_id]
        oplog_entries = transaction.oplog
        config = ConfigCache()
        for index, replica in enumerate(random.shuffle(config.replicas)):
            if index < config.count:
                response = requests.post(replica["url"] + f"/api/v1/db/replicate", json=oplog_entries)

                if response.status_code == 400:
                    raise HTTPException(status_code=400, detail=response.reason)

    def commit(self, transaction_id, replicate=True):
        t: Transaction = self.transactions[transaction_id]
        if replicate:
            self.send_oplog_entries(transaction_id)
        t.commit()

import logging
import pickle
import threading
import time

from db.collection import Collection
from db.transaction import (ReadUncommittedTransaction,
                            ReadCommittedTransaction,
                            RepeatableReadTransaction,
                            SerializableTransaction)

logger = logging.getLogger(__name__)


class Database:
    FILENAME = 'data/db.pickle'

    def __init__(self):
        self.collection = Collection()
        self.load_from_fs()
        thread = threading.Thread(target=self.dump_daemon())
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
            self.dump_to_fs()

    def load_from_fs(self):
        self.collection.records = Database.read()

    def begin_transaction(self):
        return self.begin_read_uncommitted_transaction()

    def begin_read_uncommitted_transaction(self):
        return self.collection.begin(ReadUncommittedTransaction)

    def begin_read_committed_transaction(self):
        return self.collection.begin(ReadCommittedTransaction)

    def begin_repeatable_read_transaction(self):
        return self.collection.begin(RepeatableReadTransaction)

    def begin_serializable_transaction(self):
        return self.collection.begin(SerializableTransaction)

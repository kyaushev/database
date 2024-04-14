# import logging

from app.db.lock_manager import LockManager

# logger = logging.getLogger(__name__)


class Collection:
    def __init__(self):
        self.next_id = 0
        self.next_doc = 0
        self.active_ids = set()
        self.records = []
        self.locks = LockManager()

    def begin(self, transaction_type):
        self.next_id += 1
        self.active_ids.add(self.next_id)
        # logger.info(f'Start transaction: {self.next_id}.')
        return transaction_type(self, self.next_id)

    def identity(self):
        self.next_doc += 1
        return self.next_doc

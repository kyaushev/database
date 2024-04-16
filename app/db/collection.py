# import logging
import threading

from app.db.lock_manager import LockManager


class Collection:
    def __init__(self, next_doc=0):
        self.lock = threading.Lock()
        self.next_id = 0
        self.next_doc = next_doc
        self.active_ids = set()
        self.records = []
        self.locks = LockManager()

    def begin(self, transaction_type):
        self.lock.acquire()
        self.next_id += 1
        self.active_ids.add(self.next_id)
        self.lock.release()
        return transaction_type(self, self.next_id)

    def identity(self):
        self.lock.acquire()
        self.next_doc += 1
        return self.next_doc

import threading


class LockManager:
    def __init__(self):
        self.lock = threading.Lock()
        self.locks = []

    def add(self, transaction, key):
        self.lock.acquire()
        if not self.exists(transaction, key):
            self.locks.append([transaction, key])
        self.lock.release()

    def exists(self, transaction, key):
        return any(lock[0] is transaction and lock[1] == key for lock in self.locks)

class LockManager:
    def __init__(self):
        self.locks = []

    def add(self, transaction, key):
        if not self.exists(transaction, key):
            self.locks.append([transaction, key])

    def exists(self, transaction, key):
        return any(lock[0] is transaction and lock[1] == key for lock in self.locks)

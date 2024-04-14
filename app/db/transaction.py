import logging
from enum import Enum

from app.db.exeptions import RollbackException

logger = logging.getLogger(__name__)


class TransactionType(str, Enum):
    read_uncommitted = 'read_uncommitted'
    read_committed = 'read_committed'
    repeatable_read = 'repeatable_read'
    serializable = 'serializable'


class Transaction:
    def __init__(self, collection, id):
        self.collection = collection
        self.id = id
        self.rollback_actions = []

    def add_record(self, name, doc):
        record = {
            '_id': self.collection.identity(),
            'name': name,
            'doc': doc,
            'created_id': self.id,
            'expired_id': 0
        }
        self.rollback_actions.append(["delete", len(self.collection.records)])
        self.collection.records.append(record)
        logger.info(f'Record {record["_id"]} added by {self.id}')
        return record['_id']

    def delete_record(self, name):
        for i, record in enumerate(self.collection.records):
            if self.is_visible(record) and record['name'] == name:
                if self.is_locked(record):
                    warn = f'Failed to delete: record {record["_id"]} locked by another transaction.'
                    logger.warning(warn)
                    raise RollbackException(warn)
                else:
                    record['expired_id'] = self.id
                    self.rollback_actions.append(["add", i])
                    logger.info(f'Record {record["_id"]} deleted by transaction: {self.id}.')

    def update_record(self, name, doc):
        logger.info(f'Update record {name}')
        self.delete_record(name)
        return self.add_record(name, doc)

    def fetch_record(self, name):
        for record in self.collection.records:
            if self.is_visible(record) and record['name'] is name:
                return record

        return None

    def count_records(self):
        return sum(self.is_visible(record) for record in self.collection.records)

    def fetch_all_records(self):
        return [record for record in self.collection.records if self.is_visible(record)]

    def fetch(self, expr):
        visible_records = []
        for record in self.collection.records:
            if self.is_visible(record) and expr(record):
                visible_records.append(record)

        return visible_records

    def commit(self):
        check_ids = [id for _, id in self.rollback_actions]
        new_records = [record
                       for i, record in enumerate(self.collection.records)
                       if i not in check_ids or not self.is_dead(record)]
        self.collection.records = new_records

        self.collection.active_ids.discard(self.id)

        logger.info(f'Commit transaction: {self.id}.')

    def rollback(self):
        for action in reversed(self.rollback_actions):
            if action[0] == 'add':
                self.collection.records[action[1]]['expired_id'] = 0
            elif action[0] == 'delete':
                self.collection.records[action[1]]['expired_id'] = self.id

        self.collection.active_ids.discard(self.id)
        logger.info(f'Rollback transaction: {self.id}.')

    def is_visible(self, record):
        pass

    def is_locked(self, record):
        pass

    def is_dead(self, record):
        return record['expired_id'] == self.id


class ReadUncommittedTransaction(Transaction):
    def is_locked(self, record):
        return not self.is_visible(record)

    def is_visible(self, record):
        return record['expired_id'] == 0


class ReadCommittedTransaction(Transaction):
    def is_locked(self, record):
        return record['expired_id'] != 0 and record['expired_id'] in self.collection.active_ids

    def is_visible(self, record):
        if record['created_id'] in self.collection.active_ids and record['created_id'] != self.id:
            return False

        if record['expired_id'] != 0 and \
                (record['expired_id'] not in self.collection.active_ids or record['expired_id'] == self.id):
            return False

        return True


class RepeatableReadTransaction(ReadCommittedTransaction):
    def is_locked(self, record):
        return ReadCommittedTransaction.is_locked(self, record) or \
            self.collection.locks.exists(self, record['_id'])

    def is_visible(self, record):
        is_visible = ReadCommittedTransaction.is_visible(self, record)

        if is_visible:
            self.collection.locks.add(self, record['_id'])

        return is_visible


class SerializableTransaction(RepeatableReadTransaction):
    def __init__(self, collection, id):
        Transaction.__init__(self, collection, id)
        self.existing_ids = self.collection.active_ids.copy()

    def is_visible(self, record):
        is_visible = ReadCommittedTransaction.is_visible(self, record) \
                     and record['created_id'] <= self.id \
                     and record['created_id'] in self.existing_ids

        if is_visible:
            self.collection.locks.add(self, record['_id'])

        return is_visible

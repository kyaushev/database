from enum import Enum

class TransactionType(str, Enum):
    read_uncommitted = 'read_uncommitted'
    read_committed = 'read_committed'
    repeatable_read = 'repeatable_read'
    serializable = 'serializable'
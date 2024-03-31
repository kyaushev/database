from exeptions import RollbackException
from collection import Collection
from transaction import ReadCommittedTransaction

import datetime

doc1 = {
    'name': {'first': 'Alan', 'last': 'Turing'},
    'birth': str(datetime.date(1912, 6, 23)),
    'death': str(datetime.date(1954, 6, 7)),
    'contribs': ['Turing machine', 'Turing test', 'Turingery']
}
doc2 = {
    'name': {'first': 'Charles', 'last': 'Babbage'},
    'birth': str(datetime.date(1791, 12, 26)),
    'death': str(datetime.date(1871, 10, 18)),
    'contribs': ['Difference engine', 'Difference Engine']
}


class TransactionTest:
    def __init__(self, transaction_type):
        self.col = Collection()
        t0 = self.col.begin(ReadCommittedTransaction)
        t0.add_record(name='Doc1', doc=doc1)
        t0.add_record(name='Doc2', doc=doc2)
        t0.commit()

        self.t1 = self.col.begin(transaction_type)
        self.t2 = self.col.begin(transaction_type)

    def run_test(self):
        try:
            return self.run()
        except RollbackException:
            return False

    def result(self):
        if self.run_test():
            return u'✔'
        return u'✘'

    def run(self):
        pass


class DirtyRead(TransactionTest):
    def run(self):
        result1 = self.t1.fetch_record(name='Doc1')
        self.t2.update_record(name='Doc1', doc=doc2)
        result2 = self.t1.fetch_record(name='Doc1')

        return result1 != result2


class NonRepeatableRead(TransactionTest):
    def run(self):
        result1 = self.t1.fetch_record(name='Doc1')
        self.t2.update_record(name='Doc1', doc=doc1)
        self.t2.commit()
        result2 = self.t1.fetch_record(name='Doc1')

        return result1 != result2


class PhantomRead(TransactionTest):
    def run(self):
        result1 = self.t1.count_records()
        self.t2.add_record(name='Doc3', doc='{}')
        self.t2.commit()
        result2 = self.t1.count_records()

        return result1 != result2

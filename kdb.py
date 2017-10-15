import datetime
import numpy
import random
import threading
import sys
import time

from qpython import qconnection, MetaData
from qpython.qcollection import qlist
from qpython.qtype import QException, QTIME_LIST, QSYMBOL_LIST, QFLOAT_LIST, QTIMESTAMP_LIST, QDATE_LIST, QKEYED_TABLE, QTABLE


class PublisherThread(threading.Thread):

    def __init__(self):
        super(PublisherThread, self).__init__()
        self.q = qconnection.QConnection(host='localhost', port=5000, pandas=True)
        self.q.open()
        print('Connected to KDB')
        self._stopper = threading.Event()

    def stop(self):
        self._stopper.set()

    def stopped(self):
        return self._stopper.isSet()

    def run(self, data):
        """Sync data with kdb"""
        data.meta = MetaData(**{'qtype': QKEYED_TABLE})
        data.reset_index(drop=True)
        data.set_index(['timestamp'], inplace=True)
        self.q.sync('insert', numpy.string_('trades'), data)
        temp = self.q('trades')
        print(len(temp))

    def commit(self):
        self.q('save `trade_hist')
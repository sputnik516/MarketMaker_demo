import numpy as np
import threading
from qpython import qconnection, MetaData
from qpython.qtype import QException, QKEYED_TABLE


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
        try:
            self.q.sync('insert', np.string_('trades'), data)
            temp = self.q('trades')
            # print(len(temp))
            print('KDB updated')
        except QException as e:
            print(str(e))

    def commit(self):
        self.q('save `trade_hist')

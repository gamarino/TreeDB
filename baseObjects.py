# Base objects

from . import exceptions


class BaseException(exceptions.DBException):
    def __init__(self, code=999, msg=''):
        super.__init__(code='BASE%3d' % code, msg=msg)


class Transaction:
    def __init__(self):
        self.readObjects = {}

    def close(self):
        pass

    def getAt(self, atomId):
        raise BaseException(1, 'Not implemented')

    def write(self, atom):
        raise BaseException(1, 'Not implemented')

    def lockRoot(self):
        raise BaseException(1, 'Not implemented')

    def close(self, newRoot=None):
        raise BaseException(1, 'Not implemented')


class Storage:
    def OpenWriteTransaction(self):
        """
        Opens a new write transaction.
        :return: the new write transaction object
        """
        raise BaseException(1, 'Not implemented')

    def OpenReadTransaction(self, transactionId):
        """
        Opens a new read transaction.
        :return: the new write transaction object
        """
        raise BaseException(1, 'Not implemented')



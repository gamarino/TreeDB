# Base objects

from . import exceptions

import uuid
import hashlib

class BaseException(exceptions.DBException):
    def __init__(self, code=999, msg=''):
        super(self).__init__(code='BASE%3d' % code, msg=msg)


class Atom:
    def __init__(self, atomId=None):
        self.atomId = atomId

class Root:
    def __init__(self):
        super().__init__()

        self.userRoot = None
        self.salt = uuid.uuid4()
        self.password = None

    def copyTo(self, otherRoot):
        otherRoot.userRoot = self.userRoot
        otherRoot.salt = self.salt
        otherRoot.password = self.password

    def setPassword(self, newPassword):
        # TODO
        pass


class StorageTransaction:
    def __init__(self):
        self.readObjects = {}
        self.rootId = None

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
    def newWriteTransaction(self):
        """
        Opens a new write transaction.
        :return: the new write transaction object
        """
        raise BaseException(1, 'Not implemented')

    def newReadTransaction(self):
        """
        Opens a new read transaction.
        :return: the new write transaction object
        """
        raise BaseException(1, 'Not implemented')



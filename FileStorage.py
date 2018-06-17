# File storage

from . import exceptions
from . import baseObjects

import sys
import os

from yaml import load, dump
try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

import datetime
import random
import uuid
import time


TRANSACTION_FILENAME_MAXCACHE = 2 # Max number of seconds to renew info about transaction files
RETRY_ROOT_LOCK_TIME = 500 # Max number of milliseconds to wait before retrying to lock root
RETRY_MAX_COUNT = 5
FORMAT_VERSION = '0.1'


class FileStorageException(exceptions.DBException):
    def __init__(self, code=999, msg=''):
        super().__init__(code='FS%3d' % code, msg=msg)


class FileRoot(baseObjects.Root):
    def __init__(self):
        super().__init__()
        self.version = '0.1'
        self._currentRootFilename = None
        self._currentRootOffset = 0

    def reload(self, fs):
        rootFile = open(os.path.join(fs.directory, 'root.yaml'), 'r')
        try:
            existingRoot = load(rootFile)
            self._currentRootFilename = existingRoot.currentRootFilename
            self._currentRootOffset = existingRoot.currentRootOffset
            self.state = existingRoot.state
        finally:
            rootFile.close()

    def copyTo(self, otherRoot):
        super().copyTo(otherRoot)
        otherRoot.version = self.version
        otherRoot._currentRootFilename = self._currentRootFilename
        otherRoot._currentRootOffset = self._currentRootOffset


class ReadFileTransaction(baseObjects.StorageTransaction):
    def __init__(self, fs):
        super().__init__()
        self.fs = fs

        currentRoot = FileRoot()
        currentRoot.reload(self.fs)
        self.rootId = (currentRoot._currentRootFilename, currentRoot._currentRootFilename)
        self.userRootId = currentRoot.userRoot.atomId
        self.openFiles = {}
        self.state = 'open'

    def getAt(self, atomId):
        if self.state != 'open':
            raise FileStorageException(1, 'Invalid state of transaction for a read')

        fileName, offset = atomId
        if fileName not in self.openFiles:
            self.openFiles[fileName] = open(os.path.join(self.fs.directory, fileName), 'r')
        fd = self.openFiles[fileName]
        fd.seek(offset)
        return load(fd)

    def lockRoot(self):
        pass

    def close(self, newRoot=None):
        if newRoot:
            raise FileStorageException(3, 'On a read transaction, you can not change the root!')

        for fd in self.openFiles.items():
            fd.close()

        self.state = 'closed'


class WriteFileTransaction(ReadFileTransaction):
    def __init__(self, fs, writeFileName, writeFd):
        super().__init__(fs)
        self.writeFileName = writeFileName
        self.writeFd = writeFd
        self.state = 'open'

    def write(self, data):
        if self.state != 'open':
            raise FileStorageException(1, 'Invalid state of transaction for a write')
        offset = self.writeFd.tell()
        dump(data, self.writeFd, explicit_start=True)
        return (self.writeFileName, offset)

    def lockRoot(self):
        if self.state != 'open':
            raise FileStorageException(1, 'Invalid state of transaction for a lockRoot')

        self.state = 'closing'
        self.rootFile = None

        retryCount = 0
        while not self.rootFile and retryCount < RETRY_MAX_COUNT:
            rootFileName = os.path.join(self.fs.directory, 'root.yaml')
            isNewRoot = os.path.isfile(rootFileName)
            self.rootFile = open(rootFileName, 'w')
            if self.rootFile:
                currentRoot = None
                if not isNewRoot:
                    fd = open(os.path.join(self.fs.directory, 'root.yaml'), 'r')
                    currentRoot = load(fd)
                    fd.close()
                self.commitRoot = currentRoot
            else:
                time.sleep(random.randint(RETRY_ROOT_LOCK_TIME) / 1000.0)
                retryCount += 1

        if not self.rootFile:
            raise FileStorageException(2, 'FAILURE trying to lock root')

        return (self.commitRoot.currentRootFilename, self.commitRoot.currentRootOffset)

    def close(self, newRootId=None):
        if self.state != 'closing':
            raise FileStorageException(1, 'Invalid state of transaction for a close')

        if not self.rootFile:
            raise FileStorageException(4, 'You should call lockRoot before closing!')

        self.writeFd.close()

        fileName, offset = newRootId
        baseRootFilename, baseRootOffset = self.rootId
        if fileName != baseRootFilename or offset != baseRootOffset:
            newRoot = FileRoot()
            if self.commitRoot:
                self.commitRoot.copyTo(newRoot)
            newRoot.state = 'commit'
            newRoot._currentRootFilename = fileName
            newRoot._currentRootOffset = offset

            dump(newRoot, self.rootFile)

        self.rootFile.close()

        super(self).close()
        self.state = 'closed'


class FileStorage(baseObjects.Storage):
    def __init__(self, directory, initialPassword):
        super().__init__()
        self.directory = directory
        self.fileName = None

        self.transactionFileCache = []
        self.transactionFileCacheTimestamp = None

        if not os.path.exists(directory):
            os.makedirs(directory)

            newRoot = FileRoot()
            newRoot.setPassword(initialPassword)
            tr = self.newWriteTransaction()
            newRootId = tr.write(newRoot)
            tr.lockRoot()
            tr.close(newRootId)


    def openAvailableTransactionFile(self):
        if not self.transactionFileCache or not self.transactionFileCacheTimestamp or \
           (datetime.datetime.now() - self.transactionFileCacheTimestamp).seconds > TRANSACTION_FILENAME_MAXCACHE:
            self.transactionFileCache = [f for f in
                                         os.listdir(self.directory) if os.path.isfile(os.path.join(self.directory, f)) \
                                                                       and f != 'root.yaml']
        filesToTry = list(self.transactionFileCache)
        fd = None
        fileName = None
        while len(filesToTry) > 0:
            fileName = filesToTry[random.randint(len(filesToTry))]
            fd = open(fileName, 'w+')
            if fd:
                break
            filesToTry.remove(fileName)

        if not fd:
            # No available transaction file
            # Create a new one
            fileName = str(uuid.uuid4())
            fd = open(fileName, 'w+')

        return fileName, fd

    def newReadTransaction(self):
        return ReadFileTransaction(self)

    def newWriteTransaction(self):
        fileName, fd = self.openAvailableTransactionFile()
        transactionFlag = {
            'Transaction': fd.tell(),
            'Timestamp': datetime.datetime.utcnow().strftime(),
        }
        dump(transactionFlag, fd, explicit_start=True)
        return WriteFileTransaction(self, fileName, fd)



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

class FileStorageException(exceptions.DBException):
    def __init__(self, code=999, msg=''):
        super.__init__(code='FS%3d' % code, msg=msg)


class Root:
    def __init__(self, fs, ):
        self.version = '0.1'
        self.currentRootFilename = None
        self.currentRootOffset = 0
        self.state = 'init'
        self.fs = fs
        if not os.path.exists(fs.directory):
            os.makedirs(fs.directory)
            rootFile = open(os.path.join(self.fs.directory, 'root.yaml'), 'w')
            try:
                dump(self, rootFile)
            finally:
                rootFile.close()
        else:
            self.reload()

    def reload(self):
        rootFile = open(os.path.join(self.fs.directory, 'root.yaml'), 'r')
        try:
            existingRoot = load(rootFile)
            self.currentRootFilename = existingRoot.currentRootFilename
            self.currentRootOffset = existingRoot.currentRootOffset
            self.state = existingRoot.state
        finally:
            rootFile.close()

    def setNewRoot(self, transaction):
        self.currentRoot = transaction.standardId()


class ReadFileTransaction(baseObjects.Transaction):
    def __init__(self, fs):
        self.fs = fs
        self.fs.root.reload()
        self.rootFilename = self.fs.root.currentRootFilename
        self.rootOffset = self.fs.root.currentRootOffset
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
        super.__init__(fs)
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
            self.rootFile = open(os.path.join(self.fs.directory, 'root.yaml'), 'r')
            if not self.rootFile:
                time.sleep(random.randint(RETRY_ROOT_LOCK_TIME) / 1000.0)
                retryCount += 1

        if not self.rootFile:
            raise FileStorageException(2, 'FAILURE trying to lock root')

    def close(self, newRootId):
        if self.state != 'closing':
            raise FileStorageException(1, 'Invalid state of transaction for a close')

        if not self.rootFile:
            raise FileStorageException(4, 'You should call lockRoot before closing!')

        self.writeFd.close()

        fileName, offset = newRootId
        if fileName != self.root.currentRootFilename or \
           offset != self.root.currentRootOffset:

            self.root.currentRootFilename = fileName
            self.root.currentRootOffset = offset

            dump(self.root, self.rootFile)

        self.rootFile.close()

        super.close()
        self.state = 'closed'


class FileStorage(baseObjects.Storage):
    def __init__(self, directory):
        self.directory = directory
        self.fileName = None

        if not os.path.exists(directory):
            os.makedirs(directory)
            root = Root(self)
            root.update()
        else:
            root = Root(self)

        self.root = root
        self.transactionFileCache = []
        self.transactionFileCacheTimestamp = None

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

    def OpenReadTransaction(self):
        return ReadFileTransaction(self)

    def OpenWriteTransaction(self):
        fileName, fd = self.openAvailableTransactionFile()
        transactionFlag = {
            'Transaction': fd.tell(),
            'Timestamp': datetime.datetime.utcnow().strftime(),
        }
        dump(transactionFlag, fd, explicit_start=True)
        return WriteFileTransaction(self, fileName, fd)



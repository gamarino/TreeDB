# DB operations

from . import exceptions
from . import baseObjects

import random
import uuid
import time

MAX_COMMIT_RETRY = 10
COMMIT_DELAY = 100 # in milliseconds


class DBException(exceptions.DBException):
    def __init__(self, code=999, msg=''):
        super().__init__(code='DB%3d' % code, msg=msg)


class DBDatabase:
    def __init__(self, storage):
        self.storage = storage

    def newSession(self, credentials):
        # TODO
        pass

    def close(self):
        # TODO
        pass

    def newReadTransaction(self):
        return self.storage.newReadTransaction()

    def newWriteTransaction(self):
        return self.storage.newWriteTransaction()


class DBSession:
    def __init__(self, database, credentials):
        self.database = database
        # TODO validate credentials
        pass

    def newReadTransaction(self):
        return self.database.newReadTransaction()

    def newWriteTransaction(self):
        return self.database.newWriteTransaction()


class DBTransaction:
    def __init__(self, session, forUpdate=False):
        self.storage = session
        self.forUpdate = forUpdate
        self.sTransaction = session.newReadTransaction() if not forUpdate else \
                            session.newWriteTransaction()
        self.baseRoot = self.sTransaction.getAt(self.sTransaction.getRoot())
        self.newUserRoot = None
        self.readObjects = {}
        self.state = 'open'

    def getAt(self, dbObjectId):
        if dbObjectId not in self.readObjects:
            obj = self.baseRoot.getAt(dbObjectId)
            self.readObjects[dbObjectId] = obj
            return obj
        else:
            return self.readObjects[dbObjectId]

    def write(self, dbObject):
        return self.sTransaction.write(dbObject)

    def commit(self):
        if self.forUpdate:
            if self.state != 'open':
                raise DBException(2, 'Transaction not open to commit')

            toUpdateObjects = [o for o in self.readObjects if o.isDirty]
            if self.newUserRoot:
                toUpdateObjects.append(self.newUserRoot)

            for uo in toUpdateObjects:
                uo.save()

            commitRetry = 0
            while commitRetry < MAX_COMMIT_RETRY:
                commitRoot = self.sTransaction.lockRoot()

                for uo in toUpdateObjects:
                    commitRoot.setAt(uo.dbObjectId, uo.atomId)
                commitRoot.save()

                if self.sTransaction.close(commitRoot.atomId, self.newUserRoot, commitRoot.atomId):
                    self.state = 'done'
                    return
                else:
                    commitRetry += 1
                    time.sleep(random.randint(COMMIT_DELAY / 1000.0))

            self.state = 'aborted'
            raise DBException(1, 'Transaction could not be commited')

    def abort(self):
        self.sTransaction.abort()
        self.state = 'aborted'

    def getUserRoot(self):
        if self.state != 'open':
            raise DBException(2, 'Transaction not open to access')

        return self.baseRoot.userRoot if not self.newUserRoot else self.newUserRoot

    def setUserRoot(self, newUserRoot):
        self.newUserRoot = newUserRoot


class DBParentNode(baseObjects.Atom):
    def __init__(self, tr, parent=None, proxyParent=None):
        super().__init__()
        self._tr = tr
        self.parent = parent
        self.proxyParent = proxyParent
        self._loaded = False
        self._dirty = True

    def load(self):
        if self.atomId:
            loadedDBParentNode = self.tr.getAt(self.atomId)
            self.parent = loadedDBParentNode.parent
            self.dirty = False
        self.loaded = True

    def __getattribute__(self, item):
        if not self._loaded:
            self.load()

        if self.proxyParent.hasattribute(item):
            return self.proxyParent.__getattribute__(item)

        return self.parent.__getattribute__(item)

    def hasattribute(self, item):
        if not self._loaded:
            self.load()

        if self.proxyParent.hasattribute(item):
            return True

        if self.parent:
            return self.parent.hasattribute(item)

        return False


class DBObject(baseObjects.Atom):
    def __init(self, tr, atomId=None, parent=None):
        super().__init__(atomId)
        self._tr = tr
        self.parent = parent
        self._attributes = {}
        self._loaded = False
        self._dirty = True
        self._dbObjectId = None
        self._saved = False

    def __getattribute__(self, item):
        if not self._loaded:
            self.load()

        if item in self._attributes:
            return self._attributes[item]

        if self._parent_id:
            return self._parent_id.__getattribute__(item)

        return None

    def hasattribute(self, item):
        if item in self._attributes:
            return True

        if self._parent_id:
            return self._parent_id.hasattribute(item)

        return False

    def __setattr__(self, key, value):
        self._attributes[key] = value
        self._dirty = True

    def setParent(self, newParent):
        if self.parent:
            self.parent = DBParentNode(self._tr, parent=self.parent, proxyParent=newParent)
        else:
            self.parent = newParent
        self.dirty = True

    def load(self):
        if self.atomId:
            loadedDBObject = self._tr.getAt(self.atomId)
            self.parent = loadedDBObject.parent
            self._attributes = loadedDBObject.attributes
            self.dirty = False
        self.loaded = True

    def save(self):
        if self.dirty and not self.saved:
            if not self._dbObjectId:
                while True:
                    newObjectId = uuid.uuid4()
                    if not self._tr.getAt(newObjectId):
                        break

                self._dbObjectId = newObjectId

            # prevent cyclic recursion
            self._saved = True

            # Save all referenced attributes
            for att in self._attributes.items:
                if att and isinstance(att, DBObject):
                    att.save()

            # Write to storage transaction
            self.atomId = self._tr.write(self)

    def clone(self):
        # TODO
        pass

    def new(self):
        # TODO
        pass


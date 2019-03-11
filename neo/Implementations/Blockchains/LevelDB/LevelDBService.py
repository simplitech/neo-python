import plyvel
from neocore.Core.State.AccountState import AccountState
from neocore.Core.State.AssetState import AssetState
from neocore.Core.State.CoinState import CoinState
from neocore.Core.State.ContractState import ContractState
from neocore.Core.State.SpentCoinState import SpentCoinState
from neocore.Core.State.StorageItem import StorageItem
from neocore.Core.State.ValidatorState import ValidatorState
from neocore.IO.DBCollection import DBCollection
from neocore.IO.DBService import DBService

from neocore.logging import log_manager
logger = log_manager.getLogger('db')

class LevelDBService(DBService):
    def __init__(self, storagePath : str, createIfMissing = True):
        self.storagePath = storagePath
        self.createIfMissing = createIfMissing
        self._persisting_block = False
        try:
            self._db = plyvel.DB(self.storagePath, create_if_missing=True)
            logger.info("Created Blockchain DB at %s " % self.storagePath)
        except Exception as e:
            logger.info("leveldb unavailable, you may already be running this process: %s " % e)
            raise Exception('Leveldb Unavailable')

    def getCurrentBlock(self):
        if self._persisting_block:
            return self._persisting_block
        return self.GetBlockByHeight(self.Height)

    def getCurrentHeader(self):
        return self.get(self.getKeyCurrentHeader())

    def getSystemVersion(self):
        return self.get(self.getKeySystemVersion())

    def updateSystemVersion(self, sysVersion):
        self._db.put(self.getKeySystemVersion(), sysVersion)

    def getAccountsCollection(self) -> DBCollection:
        return DBCollection(self, self.getPrefixAccount(), AccountState)

    def getCoinsCollection(self) -> DBCollection:
        return DBCollection(self, self.getPrefixCoin(), CoinState)

    def getSpentCoinsCollection(self) -> DBCollection:
        return DBCollection(self, self.getPrefixSpentCoin(), SpentCoinState)

    def getValidatorsCollection(self) -> DBCollection:
        return DBCollection(self, self.getPrefixValidator(), ValidatorState)

    def getAssetsCollection(self) -> DBCollection:
        return DBCollection(self, self.getPrefixAsset(), AssetState)

    def getContractCollection(self) -> DBCollection:
        return DBCollection(self, self.getPrefixContract(), ContractState)

    def getStorageCollection(self) -> DBCollection:
        return DBCollection(self, self.getPrefixStorage(), StorageItem)

    def getDBIterator(self):
        return self._db.iterator()

    def getSnapshot(self):
        return self._db.snapshot()

    def getDbIteratorForPrefix(self, prefix, include_value = False):
        return self._db.iterator(prefix = prefix)

    def getHeaderListIterator(self):
        return self._db.iterator(prefix = self.getPrefixHeaderHashList())

    def getBlockListIterator(self):
        return self._db.iterator(prefix = self.getPrefixBlock())

    def getStorageListIterator(self, includeValue = False):
        return self._db.iterator(prefix=self.getPrefixStorage(), includeValue = includeValue)

    def getWriter(self):
        return self._db.write_batch()

    def close(self):
        self._db.close()
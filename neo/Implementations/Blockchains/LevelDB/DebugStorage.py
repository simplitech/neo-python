from neocore.Core.Blockchain import Blockchain

import plyvel
from neo.Settings import settings
from neocore.logging import log_manager

logger = log_manager.getLogger('db')


class DebugStorage:
    __instance = None

    @property
    def db(self):
        return self._db

    def reset(self):
        for key in Blockchain.GetInstance().nodeServices.dbService.getStorageListIterator():
            self._db.delete(key)

    def clone_from_live(self):
        clone_db = Blockchain.GetInstance().nodeServices.dbService.getSnapshot()
        for key, value in clone_db.iterator(prefix=Blockchain.GetInstance().nodeServices.dbService.getPrefixStorage(), include_value=True):
            self._db.put(key, value)

    def __init__(self, db):

        try:
            self._db = plyvel.DB(settings.debug_storage_leveldb_path, create_if_missing=True)
        except Exception as e:
            logger.info("DEBUG leveldb unavailable, you may already be running this process: %s " % e)
            raise Exception('DEBUG Leveldb Unavailable %s ' % e)

    @staticmethod
    def instance():
        if not DebugStorage.__instance:
            DebugStorage.__instance = DebugStorage()
            DebugStorage.__instance.clone_from_live()
        return DebugStorage.__instance

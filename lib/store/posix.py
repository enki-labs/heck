"""
A store using the local file system.

"""

import os
from datetime import datetime
import errno
from lib import store

store_base = os.environ["HECK_STORE_BASE"]

class Store (store.Store):
    """
    Local file system store.
    """

    def __init__ (self):
        """ Connect to store """
        self._base = store_base
        if not os.path.isdir(self._base):
            raise Exception("Store base path (%s) does not exist" % (self._base))

    def _full_path (self, path):
        """ Return a fully qualified path """
        return os.path.join(self._base, path)

    def _sub_path (self, path):
        """ Strip the base path from a full path """
        return os.path.relpath(path, self._base)

    def mkdir (self, path):
        try:
            os.makedirs(self._full_path(path))
        except OSError as ex:
            if ex.errno == errno.EEXIST and os.path.isdir(self._full_path(path)):
                pass
            else:
                raise

    def read (self, path, open_handle):
        return StoreFile(self._full_path(path), "r", open_handle)

    def append (self, path, open_handle):
        return StoreFile(self._full_path(path), "a", open_handle)

    def write (self, path, open_handle):
        return StoreFile(self._full_path(path), "w", open_handle)

    def exists (self, path):
        return os.path.exists(self._full_path(path))
    
    @staticmethod
    def _file_info(directory, fname):
        """ Extract info for a given file """
        stats = os.stat(os.path.join(directory, fname))
        modify = datetime.fromtimestamp(stats.st_mtime)
        return dict(name=fname, modify=modify, size=stats.st_size)

    def walk (self, path, visitor, recursive = False):
        if recursive:
            for directory, dirs, files in os.walk(self._full_path(path)):
                for fi in files:
                    visitor(self._sub_path(directory), Store._file_info(directory, fi))
        else:
            for fi in os.listdir(self._full_path(path)):
                visitor(path, Store._file_info(directory, fi))


import time

class StoreFile (store.StoreFile):
    """
    Wrap a local file.
    """

    def __init__ (self, path, mode, open_handle):
        """ Open file in mode 'r' read or 'w' write """
        from lib import common
        self._path = path
        if mode not in ['r', 'w', 'a']: raise Exception("Unsupported mode %s" % mode)
        self._mode = mode
        self._open_handle = open_handle
        self._file = None
        self._lock = common.ResourceLock(self._path)

    def __enter__ (self):
        from lib import common
        self._lock.acquire()
        common.log.info("locked file (%s)" % (self._path))
        if self._open_handle:     
            self._file = open(self._path, "%sb" % (self._mode))
        return self

    def __exit__ (self, typ, value, tb):
        from lib import common
        if self._file:
            self._file.close()
        self._lock.release()
        common.log.info("released file (%s)" % (self._path))

    def local (self):
        return self._file

    def local_path (self):
        return self._path

    def save (self):
        from lib import common
        common.log.info("save file (%s)" % (self._path))
        if self._mode == 'r':
            raise Exception("File in read mode - cannot write")
        if self._file:
            self._file.flush()


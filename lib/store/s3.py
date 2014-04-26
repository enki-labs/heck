"""
A store using the local file system.

"""

import botocore.session
import os
from datetime import datetime
import errno
from lib import store

store_cache = os.environ["HECK_STORE_CACHE"]
store_bucket = os.environ["HECK_AWS_BUCKET"]
store_region = os.environ["HECK_AWS_REGION"]
store_key = os.environ["HECK_AWS_KEY"] 
store_secret = os.envrion["HECK_AWS_SECRET"]


class Store (store.Store):
    """
    Local file system store.
    """

    def __init__ (self):
        """ Connect to store """
        self._cache = store_cache
        if not os.path.isdir(self._cache):
            raise Exception("Store cache path (%s) does not exist" % (self._cache))

        self._session = botocore.session.get_session()
        self._session.set_credentials(store_key, store_secret)
        self._service = self._session.get_service("s3")
        self._endpoint = self._service.get_endpoint(store_region)
        self._put = self._service.get_operation("PutObject")
        self._get = self._service.get_operation("GetObject")

    def _local_path (self, path):
        """ Return a fully qualified local path """
        return os.path.join(self._cache, path)

    def internal_put (self, key, handle):
        """ Put the file to S3 """
        from lib import common
        common.log.info("put on S3 %s" % (key))
        res_http, res_data = self._put.call(self._endpoint, bucket=store_bucket, key=key, body=handle)

    def internal_get (self, key, handle):
        """ Get the file from S3 """
        from lib import common
        common.log.info("get from S3 %s" % (key))
        buff = 1024 * 8
        res_http, res_data = self._get.call(self._endpoint, bucket=store_bucket, key=key)
        data = res_data["Body"].read(buff)
        while data:
            handle.write(data)
            data = res_data["Body"].read(buff)

    #def _sub_path (self, path):
    #    """ Strip the base path from a full path """
    #    return os.path.relpath(path, self._base)

    def mkdir (self, path):
        pass
        #try:
        #    os.makedirs(self._full_path(path))
        #except OSError as ex:
        #    if ex.errno == errno.EEXIST and os.path.isdir(self._full_path(path)):
        #        pass
        #    else:
        #        raise

    def read (self, path, open_handle):
        return StoreFile(self, path, self._local_path(path), "r", open_handle)

    def append (self, path, open_handle):
        raise NotImplemented()
        return StoreFile(self, path, self._local_path(path), "a", open_handle)

    def write (self, path, open_handle):
        raise NotImplemented()
        return StoreFile(self, path, self._local_path(path), "w", open_handle)

    def exists (self, path):
        raise NotImplemented()
        return os.path.exists(path, self._local_path(path))
    
    @staticmethod
    def _file_info(directory, fname):
        """ Extract info for a given file """
        raise NotImplemented()
        stats = os.stat(os.path.join(directory, fname))
        modify = datetime.fromtimestamp(stats.st_mtime)
        return dict(name=fname, modify=modify, size=stats.st_size)

    def walk (self, path, visitor, recursive = False):
        raise NotImplemented()
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

    def __init__ (self, store, path, local_path, mode, open_handle):
        """ Open file in mode 'r' read or 'w' write """
        from lib import common
        self._store = store
        self._path = path
        self._local_path = local_path
        if mode not in ['r', 'w', 'a']: raise Exception("Unsupported mode %s" % mode)
        self._mode = mode
        self._open_handle = open_handle
        self._file = None
        self._lock = common.ResourceLock(self._path)

    def __enter__ (self):
        from lib import common
        self._lock.acquire()
        if self._mode == "r" and not os.path.exists(self._local_path):
            os.makedirs(os.path.dirname(self._local_path), exist_ok=True)
            with open(self._local_path, "wb") as handle: #cache locally
                self._store.internal_get(self._path, handle)
        if self._open_handle:               
            self._file = open(self._path, "%sb" % (self._mode))
        return self

    def __exit__ (self, typ, value, tb):
        from lib import common
        if self._file:
            self._file.close()
        self._lock.release()

    def local (self):
        return self._file

    def local_path (self):
        return self._local_path

    def save (self):
        from lib import common
        raise NotImplemented()
        #if self._mode == 'r':
        #    raise Exception("File in read mode - cannot write")
        #if self._file:
        #    self._file.flush()


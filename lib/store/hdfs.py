"""
A store using HDFS.

"""

import os
from datetime import datetime
from pywebhdfs.webhdfs import PyWebHdfsClient, errors
from lib import store

store_host = os.environ["HECK_STORE_HOST"]
store_port = os.environ["HECK_STORE_PORT"]
store_user = os.environ["HECK_STORE_USER"]

class Store (store.Store):
    """
    HDFS backed store.
    """

    def __init__ (self):
        """ Connect to store """
        self._client = PyWebHdfsClient(host=store_host, port=store_port, user_name=store_user)

    def mkdir (self, path):
        self._client.make_dir(path)

    def read (self, path):
        return StoreFile(self._client, path, "r")

    def append (self, path):
        return StoreFile(self._client, path, "a")

    def write (self, path):
        return StoreFile(self._client, path, "w")

    def exists (self, path):
        try:
            dirinfo = self._client.list_dir(path)
            return True
        except errors.FileNotFound:
            return False
    
    def walk (self, path, visitor, recursive = False):
        """ Walk files in a path. Use recursive=True to include subdirs """
        dirinfo = self._client.list_dir(path)
        for status in dirinfo["FileStatuses"]["FileStatus"]:
            if recursive and status["type"] == "DIRECTORY":
                if len(path) > 0:
                    self.walk(path + "/" + status["pathSuffix"], visitor, recursive)
                else:
                    self.walk(status["pathSuffix"], visitor, recursive)
            else:
                info = dict(name=status["pathSuffix"], 
                            modify=datetime.fromtimestamp(status["modificationTime"]), 
                            size=status["length"])
                visitor(path, info)


class StoreFile (store.Store):
    """
    Local handler for HDFS file.
    """

    def __init__ (self, client, path, mode):
        """ Open file in mode 'r' read, 'w' write, or 'a' append """
        self._client = client
        self._path = path
        if mode not in ['r', 'w', 'a']: raise Exception("Unsupported mode %s" % mode)
        self._mode = mode
        self._temp = None

    def __enter__ (self):
        """ Creates temporary file and inits with store content if read mode """
        self._temp = tempfile.NamedTemporaryFile()
        if self._mode == 'r' or (self._mode == 'a' and store.exists(self._path)):
           # -copyToLocal doesn't support overwrite so let's swap the temp file
           copytolocal_overwrite_hack = self._temp.name + ".read"
           try:
               proc = subprocess.Popen(['hdfs', 'dfs', '-copyToLocal', "/" + self._path, copytolocal_overwrite_hack], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
               out = proc.communicate()
               if proc.returncode != 0:
                   raise Exception("Read file from store failed (%s)" % (out[1]))
           except OSError as ex:
               raise Exception("Read file from store failed (%s)" % (ex.strerror))
           self._temp.close()
           filemode = "rb" if self._mode == 'r' else "ab"
           newfile = open(copytolocal_overwrite_hack, filemode)
           self._temp = tempfile._TemporaryFileWrapper(newfile, newfile.name)
        return self

    def __exit__ (self, typ, value, tb):
        """ Close and remove temp file """
        if self._temp:
            self._temp.close()

    def local (self):
        """ Get local temp file instance """
        return self._temp.file

    def save (self):
        """ Write file to store """
        if self._mode == 'r':
            raise Exception("File in read mode - cannot write")
        self._temp.flush()
        try:
            proc = subprocess.Popen(['hdfs', 'dfs', '-copyFromLocal', '-f', self._temp.name, "/" + self._path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out = proc.communicate()
            if proc.returncode != 0:
                raise Exception("Store file failed (%s)" % (out[1]))
        except OSError as ex:
            raise Exception("Store file failed (%s)" % (ex.strerror))



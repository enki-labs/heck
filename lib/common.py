"""
Common config, logging etc.

"""


import os
import sys
import tempfile
import subprocess
import logging
import autologging
import base64
from datetime import datetime
import time
from autologging import logged, traced, TracedMethods
from pywebhdfs.webhdfs import PyWebHdfsClient


""" Global environment """
store_host = os.environ["HECK_STORE_HOST"]
store_port = os.environ["HECK_STORE_PORT"]
store_user = os.environ["HECK_STORE_USER"]
db_connection = os.environ["HECK_DB"]


log = logging.getLogger("heck")
log_level = logging.DEBUG #autologging.TRACE
log.setLevel(log_level)
__stdout_handler = logging.StreamHandler(sys.stdout)
__stdout_handler.setLevel(log_level)
__formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
__stdout_handler.setFormatter(__formatter)
log.addHandler(__stdout_handler)


class Time (object):
    """
    Wrap time.
    """

    nano_per_second = 1000000000
    """ Tick times are in nanoseconds since 1970-01-01 """

    @staticmethod
    def tick (python_time=None):
        """
        Convert a Python time to a tick time.
        If no args or pyton_time is None then return current tick time.
        """
        if python_time:
            return ((time.mktime(python_time.timetuple())*Time.nano_per_second) + 
                    int(python_time.microsecond*1000))
        else:
            return Time.tick(Time.time())

    @staticmethod
    def time (tick=None):
        """
        Convert a tick time to a Python time.
        If no args or tick is None then return current Python time.
        """
        if tick:
            seconds = int(tick/Time.nano_per_second)
            microseconds = int((tick%Time.nano_per_second)/1000)
            return datetime.fromtimestamp(seconds).replace(microsecond=microseconds)
        else:
            return datetime.utcnow() #TODO: add timezone?


class StoreFile (object):
    """
    Local wrapper for a store file.
    """

    def __init__ (self, client, path, mode):
        """ Open file in mode 'r' read or 'w' write """
        self._client = client
        self._path = path
        if mode not in ['r', 'w']: raise Exception("Unsupported mode %s" % mode)
        self._mode = mode
        self._temp = None

    def __enter__ (self):
        """ Creates temporary file and inits with store content if read mode """
        self._temp = tempfile.NamedTemporaryFile()
        if self._mode == 'r':
           # -copyToLocal doesn't support overwrite so let's swap the temp file
           copytolocal_overwrite_hack = self._temp.name + ".read"
           try:
               proc = subprocess.Popen(['hdfs', 'dfs', '-copyToLocal', self._path, copytolocal_overwrite_hack], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
               out = proc.communicate()
               if proc.returncode != 0:
                   raise Exception("Read file from store failed (%s)" % (out[1]))
           except OSError as ex:
               raise Exception("Read file from store failed (%s)" % (ex.strerror))
           self._temp.close()
           newfile = open(copytolocal_overwrite_hack, "rb")
           self._temp = tempfile._TemporaryFileWrapper(newfile, newfile.name)
        return self

    def __exit__ (self, typ, value, tb):
        """ Close and remove temp file """
        if self._temp:
            self._temp.close()

    def local (self):
        """ Get local temp file instance. Use local().path to access file """
        return self._temp

    def save (self):
        """ Write file to store """
        if self._mode == 'r':
            raise Exception("File in read mode - cannot write")
        self._temp.flush()
        try:
            proc = subprocess.Popen(['hdfs', 'dfs', '-copyFromLocal', '-f', self._temp.name, self._path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out = proc.communicate()
            if proc.returncode != 0:
                raise Exception("Store file failed (%s)" % (out[1]))
        except OSError as ex:
            raise Exception("Store file failed (%s)" % (ex.strerror))
    

class Store (object):
    """
    File store.
    """

    def __init__ (self):
        """ Connect to store """
        self._client = PyWebHdfsClient(host=store_host, port=store_port, user_name=store_user)

    def mkdir (self, path):
        """ Make a directory including parent directories """
        self._client.make_dir(path)

    def read (self, path):
        """ Get a reader for a store file. Use in with statement """
        return StoreFile(self._client, "/" + path, 'r')

    def write (self, path):
        """ Get a writer for a store file. Use in with statement """
        return StoreFile(self._client, "/" + path, 'w')

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
                visitor(path, status)


store = Store()


class Path (object):
    """
    Paths.
    """

    """ Seperator pattern """
    _sep = "_@#_"

    @staticmethod
    def _encode (name):
        """ Safe filename """
        return base64.urlsafe_b64encode(bytes(name, 'utf-8')).decode('utf-8')

    @staticmethod
    def _decode (name):
        """ Decode safe filename """
        return base64.urlsafe_b64decode(bytes(name, 'utf-8')).decode('utf-8')

    @staticmethod
    def resolve_path (typ, info):
        if typ == "series_ohlc":
            return "data/store/ohlc/%s" % (info.id)
        else:
            raise Exception("Unknown path type (%s)" % typ)

    @staticmethod
    def get (typ, info):
        """ Get path. """
        name = ""
        root = ""

        if typ == "series_store":
            root = "data/store"
            name = "p_%s%ss_%s" % (info["provider"], Path._sep, info["symbol"])
            if "year" in info: name = "%s%sy_%s" % (name, Path._sep, info["year"])
            if "month" in info: name = "%s%sm_%s" % (name, Path._sep, info["month"])
            if "class" in info: name = "%s%sc_%s" % (name, Path._sep, info["class"])
            if "source" in info: name = "%s%so_%s" % (name, Path._sep, info["source"])
        else:
            raise Exception("Unknown path type %s" % typ)

        return root + "/" + Path._encode(name)

    @staticmethod
    def info (path):
        """ Decode info from path. """
        info = dict()
        if path.startswith("data/store/"):
            name = Path._decode(path[11:])
            for part in name.split(Path._sep):
                if part[:2] == "p_": info["provider"] = part[2:]
                if part[:2] == "s_": info["symbol"] = part[2:]
                if part[:2] == "y_": info["year"] = int(part[2:])
                if part[:2] == "m_": info["month"] = int(part[2:])
                if part[:2] == "c_": info["class"] = part[2:]
                if part[:2] == "o_": info["source"] = part[2:]
        else:
            raise Exception("Cannot parse path")

        return info        


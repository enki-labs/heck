"""
Common config, logging etc.

"""


import os
import sys
import logging
import autologging
import base64
from autologging import logged, traced, TracedMethods
from pywebhdfs.webhdfs import PyWebHdfsClient


""" Global environment """
store_host = os.environ["HECK_STORE_HOST"]
store_port = os.environ["HECK_STORE_PORT"]
store_user = os.environ["HECK_STORE_USER"]
db_connection = os.environ["HECK_DB"]


log = logging.getLogger("heck")
log.setLevel(autologging.TRACE)
__stdout_handler = logging.StreamHandler(sys.stdout)
__stdout_handler.setLevel(autologging.TRACE)
__formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
__stdout_handler.setFormatter(__formatter)
log.addHandler(__stdout_handler)


class Store (object):
    """
    File store.
    """

    def __init__ (self):
        """ Connect to store """
        self._client = PyWebHdfsClient(host=store_host, port=store_port, user_name=store_user)

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


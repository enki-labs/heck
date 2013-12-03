"""
Common config, logging etc.

"""


import sys
import logging
import autologging
from autologging import logged, traced, TracedMethods
from pywebhdfs.webhdfs import PyWebHdfsClient


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
        self._client = PyWebHdfsClient(host='localhost',port='50070', user_name='hdfs')

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

    @staticmethod
    def get (typ, name):
        """
        Get path.
        """
        return "data/" + name



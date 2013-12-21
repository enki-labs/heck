"""
Common config, logging etc.

"""


import os
import uuid
import sys
import tempfile
import subprocess
import logging
import autologging
import base64
from datetime import datetime
import time
import pytz
import calendar
from autologging import logged, traced, TracedMethods
from pywebhdfs.webhdfs import PyWebHdfsClient, errors
import exception

instanceid = str(uuid.uuid4())

""" Global environment """
store_engine = os.environ["HECK_STORE_ENGINE"]
db_connection = os.environ["HECK_DB"]


log = logging.getLogger("heck")
log_level = logging.DEBUG #autologging.TRACE
log.setLevel(log_level)
__stdout_handler = logging.StreamHandler(sys.stdout)
__stdout_handler.setLevel(log_level)
__formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
__stdout_handler.setFormatter(__formatter)
log.addHandler(__stdout_handler)

store = None
if store_engine == "hdfs":
    from lib.store import hdfs
    store = hdfs.Store()
elif store_engine == "posix":
    from lib.store import posix
    store = posix.Store()
else:
    raise Exception("Unknown storage engine (%s)" % (store_engine))


class ResourceLock (object):
    """
    Lock a named resource.
    """

    def __init__ (self, name):
        self._name = name
        self._lock = None

    def acquire (self, timeout_secs=None):
        from lib import schema
        from sqlalchemy.exc import IntegrityError
        new_lock = schema.table.lock()
        new_lock.name = self._name
        new_lock.instance = instanceid
        wait_count = 0

        while not self._lock:
            lock = schema.select_one("lock", schema.table.lock.name==self._name)
            if not lock or lock.instance != instanceid:
                new_lock.acquired = Time.tick()
                try:
                    schema.save(new_lock)
                    self._lock = new_lock
                    break
                except IntegrityError:
                    wait_count = wait_count + 0.5
                    if timeout_secs and wait_count > timeout_secs:
                        raise exception.TimeoutException("lock (%s) timed out" % (self._name))
                    else:
                        time.sleep(0.5) # wait
            elif lock and lock.instance == instanceid:
                break # already have an active lock
                      # this lock will be a dummy

    def release (self):
        from lib import schema
        if self._lock and schema:
            schema.delete(self._lock)

    def __del__ (self):
        self.release()


class ScopedResourceLock (object):
    def __init__ (self, name, timeout_secs=None):
        self._timeout = timeout_secs
        self._lock = ResourceLock(name)

    def __enter__ (self):
        self._lock.acquire(self._timeout)
        return self

    def __exit__ (self, typ, value, tb):
        self._lock.release()
    


class Time (object):
    """
    Wrap time.
    """

    nano_per_second = 1000000000
    """ Tick times are in nanoseconds since 1970-01-01 """

    @staticmethod
    def tick_parts (year, month, day, hour, minute, second, microsecond):
        """
        Tick time from date parts.
        """
        python_time = datetime(year, month, day, hour, minute, second, microsecond)
        return Time.tick(python_time)

    @staticmethod
    def tick (python_time=None):
        """
        Convert a Python time to a tick time.
        If no args or pyton_time is None then return current tick time.
        """
        if python_time:
            return ((calendar.timegm(python_time.timetuple())*Time.nano_per_second) + 
                    int(python_time.microsecond*1000))
        else:
            return Time.tick(Time.time())

    @staticmethod
    def time (tick=None):
        """
        Convert a tick time to a Python time.
        If no args or tick is None then return current Python time.
        """
        utc = None
        if tick:
            seconds = int(tick/Time.nano_per_second)
            microseconds = int((tick%Time.nano_per_second)/1000)
            utc = datetime.fromtimestamp(seconds).replace(microsecond=microseconds)
        else:
            utc = datetime.utcnow()
            
        return utc.replace(tzinfo=pytz.utc)


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
        elif typ == "series_tick":
            return "data/store/tick/%s" % (info.id)
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


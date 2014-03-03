import threading
import argparse
import json
import yaml
from twisted.internet import reactor
from twisted.web.server import Site
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
import requests
import numpy as np
import memcache
from sqlalchemy import func
from sqlalchemy import distinct
from sqlalchemy import desc
import celery
import redis
from datetime import timedelta
from babel.dates import format_timedelta

import exception
from config import prod
from lib import schema
from lib import data
from lib import common
from lib.process import DataFrameWrapper
from lib.data import spark


def get_series (args):
    #TODO add query and data caching
    search_tags = json.loads(args[b"tags"][0].decode("utf-8"))
    search_tags_ids = data.resolve_tags(search_tags, create=False)
    series = schema.table.series
    with schema.select("series", series.tags.contains(search_tags_ids)) as select:
        return select.limit(100).all()


def get_data (args):
    for s in get_series(args):
        return DataFrameWrapper(selected)

            
def handle_nan (v):
    if np.isnan(v):
        return None
    else:
        return float(v)


class Cache (object):

    def __init__ (self, args):
        self._instances = {}
        self._lock = threading.Lock()
        self._last_update = time.time()


    def add (self, chunks):#tags, rows):
        for chunk in chunks:
            tags_ids = data.resolve_tags(chunk["tags"], create=True)
            tags = data.decode_tags(tags_ids)
            tags_string = json.dumps(tags)

            if tags_string in self._instances:
                self._instances[tags_string].add(chunk["rows"])
            else:
                instance = CacheInstance(tags)
                instance.add(chunk["rows"])
                with self._lock:
                    self._instances[tags_string] = instance
        self._last_update = time.time()
            

    def flush (self, force=False):
        common.log.info("starting flush")
        flush_count = 0
        instances = []
        now = time.time()
        if not force and (now - self._last_update) > 120: #flush remaining
            force = True

        with self._lock:
            instances = list(self._instances.values())
        for instance in instances:
            flush_count += instance.flush(force)
        common.log.info("flushed %s" % flush_count)


class CacheInstance (object):

    def __init__ (self, tags):
        self._tags = tags
        self._cache = []
        self._cache_swap = threading.Lock()


    def add (self, rows):
        with self._cache_swap:
            self._cache.append(rows)


    def flush (self, force=False):

        #swap for flush
        cache = []
        with self._cache_swap:
            cached_count = len(self._cache)
            if cached_count == 0 or (cached_count < 30 and not force):
                return 0
            cache = self._cache
            self._cache = []
        
        flush_count = 0
        with data.get_writer_lock(self._tags, create=True, append=True) as writer_lock:
            for cached in cache:
                cached_count = len(cached)
                flush_count += cached_count
                if cached_count > 0:
                    try:
                        writer = writer_lock.writer(cached[0][0], cached[-1][0])
                        for row in cached:
                            writer.add(*row, raw=True)
                        writer.save(table_only=True) #update indexes
                    except exception.OverlapException:
                        common.log.info("ignore overlapping data %s" % (self._tags))
            writer_lock.save() #update file
        common.log.info("flush %s %s" % (flush_count, self._tags))
        return flush_count


class Store (Resource):
    """
    Store service REST interface.
    """

    def __init__ (self):
        super().__init__()
 
    def render_POST (self, request):
        action = request.args[b"action"][0].decode("utf-8")
        if action == "add":
            #tags = request.args[b"tags"][0].decode("utf-8")
            add_data = json.loads(request.args[b"data"][0].decode("utf-8"))
            #threads.deferToThread(cache.add, add_data)
            cache.add(add_data)
        elif action == "flush":
            cache.flush(True)
        return b""


class Internal (Resource):

    isLeaf = False

    def __init__ (self, args):
        super().__init__()
        self._args = args

    def getChild (self, name, request):
        if name == b"store":
            return Store()
        return FourOhFour()


class Index (Resource):

    isLeaf = False

    def __init__ (self, args):
        super().__init__()
        self._args = args

    def getChild (self, name, request):
        if name == b"internal":
            return Internal(self._args)
        return FourOhFour()

    def render_POST (self, request):
         return b""


class FourOhFour (Resource):

    isLeaf = True
 
    def __init__ (self):
        super().__init__()

    def getChild (self, name, request):
        return self

    def render_POST (self, request):
        request.setResponseCode(404)
        request.finish()
        return NOT_DONE_YET


from twisted.internet import threads
import time
import sys
import traceback

class PeriodicCaller:
    """The problem with doing a LoopingCall and then deferring to a
    thread is that you might have multiple copies of your function
    running simultaneously.  There are a variety of reasons this might
    be undesirable, so you can use this class instead, which will wait
    for a previous invocation to complete before running the next one.
    """
    def __init__(self, fn, args):
        self.fn, self.args = fn, args

    def _go(self):
        # bad things seem to happen when we throw an exception in the
        # thread pool... let's catch that and just log it
        try:
            self.fn(*self.args)
        except Exception as ex:
            common.log.error(traceback.format_exc())
        except:
            common.log.error(sys.exc_info()[0])
            #log.err()
        
    def _run(self):
        self.last = time.time()
        d = threads.deferToThread(self._go)
        d.addCallbacks(self._post_run)

    def _post_run(self, result):
        now = time.time()
        sleep_time = self.interval - (now - self.last)
        if sleep_time < 0:
            self._run()
        else:
            reactor.callLater(sleep_time, self._run)

    def start(self, interval, now=True):
        self.interval = interval
        self.last = time.time()
        if now:
            self._run()
        else:
            reactor.callLater(self.interval, self._post_run, None)


def periodicSequentialCall(fn, *args):
    """Periodically run `fn(*args)` in a threadpool.  unlike
:py:func:`~smap.util.periodicCallInThread`, will not run your task
concurrently with itself -- if the last invocation didn't finish in
time for your next execution, it will wait rather than running it in a
different thread.

You also need to call `start(interval)` on the result.
    """
    return PeriodicCaller(fn, args)


from twisted.web.server import GzipEncoderFactory
from twisted.web.resource import EncodingResourceWrapper
parser = argparse.ArgumentParser(description="REST API")
parser.add_argument("--port", type=int, help="API port (eg: 3010)", required=True)
args = parser.parse_args()

cache = Cache(args)
periodic_flush = periodicSequentialCall(lambda c=cache: cache.flush())
periodic_flush.start(20)

res = Index(args)
gzipped = EncodingResourceWrapper(res, [GzipEncoderFactory()])
factory = Site(gzipped)
reactor.listenTCP(args.port, factory)
reactor.run()


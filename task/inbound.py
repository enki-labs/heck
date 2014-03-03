from celery.decorators import task
import os
import sys
sys.path.append(os.getcwd())
from task import init_task
import exception

@task(bind=True)
@init_task
def test2 (self):

    with self.__t.steps():
        import json
        from lib import common
        from lib import schema
        self.__t.progress_end(30)
        import time
        for i in range(1,30):
            time.sleep(1)
            self.__t.progress()
        self.__t.ok()


@task(bind=True)
@init_task
def update (self):
    """
    Check inbound file names/dates and queue
    changed files.
    """

    with self.__t.steps():
        import json
        from lib import common
        from lib import schema

        def add_file (path, fileinfo, task):
            """ Check inbound files for new or modified files """
            task.progress()
            filepath = "%s/%s" % (path, fileinfo["name"])
            dbfile = schema.select_one("inbound", schema.table.inbound.path==filepath)
            if dbfile:
                meta = json.loads(dbfile.meta)
                if common.Time.tick(fileinfo["modify"]) != meta["modify"] or fileinfo["size"] != meta["size"]:
                    dbfile.status = "dirty"
                    meta["size"] = fileinfo["size"]
                    meta["modify"] = common.Time.tick(fileinfo["modify"])
                    dbfile.meta = json.dumps(meta)
                    schema.save(dbfile)
            else:
                dbfile = schema.table.inbound()
                dbfile.path = filepath
                dbfile.status = "dirty"
                meta = dict(modify=common.Time.tick(fileinfo["modify"]),
                        size=fileinfo["size"])
                dbfile.meta = json.dumps(meta)
                schema.save(dbfile)

        def count_files (path, fileinfo, task):
            task.progress_end_increment()

        common.store.walk("inbound", lambda p, fi, t=self.__t: count_files(p, fi, t), recursive=True)
        common.store.walk("inbound", lambda p, fi, t=self.__t: add_file(p, fi, t), recursive=True)

        self.apply_async(queue="control", countdown=30) #queue next
        self.__t.ok()


@task(bind=True)
@init_task
def queue (self):
    """ Check inbound files and queue import tasks """

    with self.__t.steps():
        from sqlalchemy import and_
        from lib import common
        from lib import schema
        inbound = schema.table.inbound
        queue_rate = 500
        update_rate = 30

        with schema.select("inbound", inbound.path.like("inbound/%"), inbound.status=="dirty") as select:
            self.__t.progress_end(select.count())
            for inbound_file in select.limit(queue_rate).all():

                if "bloomberg/ohlcv" in inbound_file.path:
                    import_bloomberg_ohlc.delay(inbound_file.path)   
                elif "bloomberg/symbol" in inbound_file.path:
                    import_bloomberg_symbol.delay(inbound_file.path)
                elif "reuters/tick" in inbound_file.path:
                    import_reuters_tick.delay(inbound_file.path)
                else:
                    continue

                schema.update("inbound"
                        , inbound.path==inbound_file.path
                        , status="queued")
                self.__t.progress()

        self.__t.ok()
        self.apply_async(queue="control", countdown=update_rate) #queue next
   

@task(bind=True)
@init_task
def import_bloomberg_symbol (self, path):
    """ Import Bloomberg symbol file """

    with self.__t.steps():
        from lib import schema
        from lib import common
        from lib.inbound import bloomberg_symbol
        inbound = schema.table.inbound

        with schema.select("inbound", inbound.path==path) as select:
            for inbound_file in select.first():
                with common.store.read(inbound_file.path) as infile:
                    bloomberg_symbol.Import.parse(infile.local(), self.__t)
        self.__t.ok()


@task(bind=True)
@init_task
def import_bloomberg_ohlc (self, path):
    """ Import Bloomberg OHLC files """

    with self.__t.steps():
        import gzip
        from lib import schema
        from lib import common
        from lib.inbound import bloomberg_ohlc
        inbound = schema.table.inbound

        with schema.select("inbound", inbound.path==path) as select:
            for inbound_file in select.all():
                common.log.info("process %s" % (inbound_file))
                with common.store.read(inbound_file.path, open_handle=True) as infile:
                    bloomberg_ohlc.Import.parse(gzip.GzipFile(fileobj=infile.local()), self.__t) 
                    inbound_file.status = "imported"
                    schema.save(inbound_file)
        self.__t.ok()


@task(bind=True)
@init_task
def import_reuters_tick (self, path):
    """ Import Reuters tick files """

    with self.__t.steps():
        import shutil
        from tempfile import NamedTemporaryFile
        import gzip
        from lib import common
        from lib import schema
        from lib.inbound import reuters_tick
        inbound = schema.table.inbound

        with schema.select("inbound", inbound.path==path) as select:
            for inbound_file in select.all():
                with NamedTemporaryFile(delete=True) as ntf:
                    with common.store.read(inbound_file.path, open_handle=True) as infile:
                        shutil.copyfileobj(gzip.open(infile.local_path()), ntf)
                        ntf.seek(0,0)
                        reuters_tick.Import.parse(ntf, self.__t)
        self.__t.ok()


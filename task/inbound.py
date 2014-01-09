from celery.decorators import task
import os
import sys
sys.path.append(os.getcwd())

@task
def update ():
    """
    Check inbound file names/dates and queue
    changed files.
    """

    import os
    import sys
    sys.path.append(os.getcwd())
    from config import prod

    import json
    from lib import common
    from lib import schema


    def add_file (path, fileinfo):
        """ Check inbound files for new or modified files """
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

    common.store.walk("inbound", add_file, recursive=True)


@task (ignore_result=True)
def queue ():
    """ Check inbound files and queue import tasks """

    import os
    import sys
    sys.path.append(os.getcwd())
    from config import prod

    from lib import schema
    from lib import common
    from sqlalchemy import and_
    inbound = schema.table.inbound

    with schema.select("inbound", inbound.path.like("inbound/%"), inbound.status=="dirty") as select:
        for inbound_file in select.all():

            if "bloomberg/ohlcv" in inbound_file.path:
                bloomberg_ohlc.delay(inbound_file.path)   
            elif "bloomberg/symbol" in inbound_file.path:
                bloomberg_symbol.delay(inbound_file.path)
            elif "reuters/tick" in inbound_file.path:
                reuters_tick.delay(inbound_file.path)
            else:
                continue

            schema.update("inbound"
                        , inbound.path==inbound_file.path
                        , status="queued")
    

@task
def bloomberg_symbol (path):
    """ Import Bloomberg symbol file """

    import os
    import sys
    sys.path.append(os.getcwd())
    from config import prod

    from lib import schema
    from lib import common
    from lib.inbound import bloomberg_symbol
    inbound = schema.table.inbound

    with schema.select("inbound", inbound.path==path) as select:
        for inbound_file in select.all():
            with common.store.read(inbound_file.path) as infile:
                bloomberg_symbol.Import.parse(infile.local())


@task
def bloomberg_ohlc (path):
    """ Import Bloomberg OHLC files """

    import os
    import sys
    sys.path.append(os.getcwd())
    from config import prod

    import gzip
    from lib import schema
    from lib import common
    from lib.inbound import bloomberg_ohlc
    inbound = schema.table.inbound

    with schema.select("inbound", inbound.path==path) as select:
        for inbound_file in select.all():
            common.log.info("process %s" % (inbound_file))
            with common.store.read(inbound_file.path, open_handle=True) as infile:
                bloomberg_ohlc.Import.parse(gzip.GzipFile(fileobj=infile.local()))
                inbound_file.status = "imported"
                schema.save(inbound_file)


@task
def reuters_tick (path):
    """ Import Reuters tick files """

    import os
    import sys
    sys.path.append(os.getcwd())
    from config import prod

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
                    reuters_tick.Import.parse(ntf)



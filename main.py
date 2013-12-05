"""
Main daemon process.

"""

import json
import gzip
import io
from config import prod
from lib import common
from lib import schema
from lib.inbound import bloomberg_ohlc


def add_file (path, fileinfo):
    """ Check inbound files for new or modified files """
    filepath = "%s/%s" % (path, fileinfo["pathSuffix"])
    dbfile = schema.select_one("inbound", path=filepath)
    if dbfile:
        meta = json.loads(dbfile.meta)
        if fileinfo["modificationTime"] != meta["modify"] or fileinfo["length"] != meta["size"]:
            dbfile.status = "dirty"
            schema.save(dbfile)
    else:
        dbfile = schema.table("inbound")
        dbfile.path = filepath
        dbfile.status = "dirty"
        meta = dict(modify=fileinfo["modificationTime"],
                    size=fileinfo["length"])
        dbfile.meta = json.dumps(meta)
        schema.save(dbfile)


common.store.walk("inbound", add_file, recursive=True)
common.store.walk("inbound", add_file, recursive=True)

with schema.select("inbound", status="dirty") as select:
    for inbound in select.all():
        meta = json.loads(inbound.meta)
        if meta["size"] > 10 * 1024 * 1024:
            common.log.debug("process %s" % inbound.path)
            with common.store.read(inbound.path) as infile:
                bloomberg_ohlc.Import.parse(gzip.GzipFile(fileobj=infile.local()))
            raise "OK!!!"
        



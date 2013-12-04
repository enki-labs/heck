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
        print(inbound.path)
        #with common.Store.read(inbound.path) as reader:
        #    bloomberg_ohlc.Import.parse(reader)
        a = common.store.read(inbound.path)
        print(a)
        #b = a.encode()
        #print(b)
        b = a
        print(len(b))
        #print(inbound.size)
        bloomberg_ohlc.Import.parse(io.BytesIO(gzip.decompress(b)))
        



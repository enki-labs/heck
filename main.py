"""
Main daemon process.

"""

import json
import gzip
import io
from config import prod
from lib import common
from lib import schema
from lib import data
from lib.inbound import bloomberg_ohlc
from lib.inbound import bloomberg_symbol


def add_file (path, fileinfo):
    """ Check inbound files for new or modified files """
    filepath = "%s/%s" % (path, fileinfo["pathSuffix"])
    dbfile = schema.select_one("inbound", schema.table.inbound.path==filepath)
    if dbfile:
        meta = json.loads(dbfile.meta)
        if fileinfo["modificationTime"] != meta["modify"] or fileinfo["length"] != meta["size"]:
            dbfile.status = "dirty"
            schema.save(dbfile)
    else:
        dbfile = schema.table.inbound()
        dbfile.path = filepath
        dbfile.status = "dirty"
        meta = dict(modify=fileinfo["modificationTime"],
                    size=fileinfo["length"])
        dbfile.meta = json.dumps(meta)
        schema.save(dbfile)


"""
common.store.walk("inbound", add_file, recursive=True)
common.store.walk("inbound", add_file, recursive=True)
"""

inbound_table = schema.table.inbound
"""
common.log.info("Processing Bloomberg symbols")
with schema.select("inbound", inbound_table.path.like("inbound/bloomberg/symbol/%"), inbound_table.status=="dirty") as select:
    for inbound in select.all():
        common.log.info(inbound.path)
        with common.store.read(inbound.path) as infile:
            bloomberg_symbol.Import.parse(infile.local())
"""

"""
common.log.info("Processing Bloomberg OHLCV")
with schema.select("inbound", inbound_table.path.like("inbound/bloomberg/ohlcv/%"), inbound_table.status=="dirty") as select:
    for inbound in select.all():
        meta = json.loads(inbound.meta)
        if meta["size"] > 10 * 1024 * 1024:
            common.log.debug("process %s" % inbound.path)
            raise "OK"
            with common.store.read(inbound.path) as infile:
                bloomberg_ohlc.Import.parse(gzip.GzipFile(fileobj=infile.local()))
            raise "OK!!!"
"""

"""
series = schema.select_one("series", schema.table.series.id==14303)
print(series)
reader = data.get_reader(series)
for row in reader.read_iter():
    print(common.Time.time(row["time"]), row["open"])
"""   


from lib.process import resample
 
search_tags = dict(format="ohlc", period="day", year="2001")
output_tags = dict(add=dict(period="week_mon"), remove=dict())

search_tags_ids = data.resolve_tags(search_tags, create=False)
series = schema.table.series
with schema.select("series", series.tags.contains(search_tags_ids)) as select:
    for selected in select.all():
        resample.Process.run(selected, dict(period="W-MON"), output_tags)

series = schema.select_one("series", schema.table.series.id==14303)
print(series)
reader = data.get_reader(series)
for row in reader.read_iter():
    print(common.Time.time(row["time"]), row["open"])

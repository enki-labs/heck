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
from lib.inbound import reuters_tick
from lib.inbound import bloomberg_symbol
from sqlalchemy import and_, not_
from lib import process


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


common.log.info("check for new inbound files")
#common.store.walk("inbound", add_file, recursive=True)

proc = schema.table.process()
proc.search_dict["include"] = {"period":"1day"}
proc.search_dict["exclude"] = {}
proc.output_dict["add"] = {"period":"week_mon"}
proc.output_dict["remove"] = {"provider":None}
proc.output_dict["params"] = {"period":"W-MON"}
proc.last_modified = 0
proc.name = "Day To Week Monday"
proc.processor = "resample"
schema.save(proc)


"""
#p = process.Process(None)
#p._tag_generator_multi()

t = schema.table.process()
t.search_dict["include"] = {"year":"2009", "symbol":"ES", "class":"index"}
t.search_dict["exclude"] = dict(month="6")
t.output_dict["add"] = dict(testing="test")
t.output_dict["remove"] = dict(provider=None)
t.id = 0
proc = process.Process(t)

series = schema.table.series

def queue (process, tags, depend):
    from collections import OrderedDict
    from sqlalchemy.exc import IntegrityError
    queued = schema.table.process_queue()
    queued.process = process
    queued.tags = tags
    queued.depend = depend
    try:
        schema.save(queued)
    except IntegrityError:
        pass

for generated in proc._generate_multi():
    try:
        with schema.select("series"
                         , series.tags.contains(data.resolve_tags(generated["tags"], create=False))) as select:
            selected = select.all()
            if len(selected) > 0:
                if selected[0].last_modified < generated["last_modified"]:
                    queue(t.id, generated["tags"], generated["depend"])
                else:
                    print("OK", generated["tags"])
            else:
                queue(t.id, generated["tags"], generated["depend"])
    except Exception:
        queue(t.id, generated["tags"], generated["depend"])
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
common.log.info("import tick files")
import shutil
from tempfile import NamedTemporaryFile

with schema.select("inbound", inbound_table.path.like("inbound/reuters/tick/%"), inbound_table.status=="dirty") as select:
    for inbound in select.all():
        common.log.info(inbound.path)
        with NamedTemporaryFile(delete=True) as ntf:
            with common.store.read(inbound.path, open_handle=True) as infile:
                shutil.copyfileobj(gzip.open(infile.local_path()), ntf)
                ntf.seek(0, 0)
                reuters_tick.Import.parse(ntf)
"""

 

"""
common.log.info("Processing Bloomberg OHLCV")
processing = "processing (%s)" % (common.instanceid)
with schema.select("inbound", inbound_table.path.like("inbound/bloomberg/ohlcv/%"), inbound_table.status=="dirty") as select:
    a = select.all()
    count = len(a)
    counter = 0
    for inbound in a:
        counter = counter + 1
        msg = "%s of %s" % (counter, count)
        inbound.status = processing
        schema.update("inbound"
                     , and_(schema.table.inbound.path==inbound.path, 
                           schema.table.inbound.status=="dirty")
                     , status=processing)
        schema.refresh(inbound)
        if inbound.status == processing: #locked ok
            common.log.debug("process %s" % inbound.path)
            with common.store.read(inbound.path, open_handle=True) as infile:
                bloomberg_ohlc.Import.parse(gzip.GzipFile(fileobj=infile.local()))
                inbound.status = "imported"
                schema.save(inbound)
        else:
            common.log.debug("%s locked try next" % inbound.path)
"""

"""
with schema.select("series") as select:
    for series in select.all():
        print(series)
        from tables import *
        from lib.data import ohlc
        filters = Filters(complevel = 9, complib = "blosc", fletcher32 = False)
        with ohlc.OhlcWriter(series, filters, 0, 0, overwrite=False, append=True) as writer:
            writer._table.cols.time.reindex_dirty()
            writer.save()
"""

"""
series = schema.select_one("series", schema.table.series.symbol=="ZA.REER.BBAS")
with data.get_reader(series) as reader:
    for row in reader.read_iter():
        print(common.Time.time(row["time"]), row["open"]) 
"""


config_yaml = """
filter:
    - floor,1000
    - cap,12500
    - step,3600,40,3
    - step,900,27.5,5
    - step,180,27.5,5

remove:
    - .*\[MKT_ST_IND\].*
    - .*[B-Zb-z]{1,2}\[ACT_TP_1\].*
    - .*IRGCOND.*
              
allow:
    - Open\|High\|
              
volFollows: false
copyLast: true
              
priceShift: 0
volumeLimit: 10000
maxTrade: 1000

validFrom: 
validTo: 

weekTimezone: Europe/Berlin
weekEnd: Friday 22:00
weekStart: Monday 07:50
"""

"""
from lib.process import filter_tick

search_tags = dict(format="tick", period="tick")
exclude_tags = dict(status="filtered")
output_tags = dict(add=dict(status="filtered"), remove=dict())

search_tags_ids = data.resolve_tags(search_tags, create=False)
exclude_tags_ids = data.resolve_tags(exclude_tags, create=False)
series = schema.table.series
with schema.select("series", series.tags.contains(search_tags_ids), not_(series.tags.contains(exclude_tags_ids))) as select:
    for selected in select.all():
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>")
        filter_tick.Process.run(selected, dict(config=config_yaml), output_tags)
        print("<<<<<<<<<<<<<<<<<<<<<<<<<<")
"""



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
"""

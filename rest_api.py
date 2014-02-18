
import json
from twisted.internet import reactor
from twisted.web.server import Site
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
import numpy as np
import memcache
from sqlalchemy import func
from sqlalchemy import distinct

from config import prod
from lib import schema
from lib import data
from lib import common
from lib.process import DataFrameWrapper
from lib.data import spark

memcache = memcache.Client(['127.0.0.1:11211'], debug=0)

def get_series (args):
    #TODO add query and data caching
    search_tags = json.loads(args[b"tags"][0].decode("utf-8"))
    search_tags_ids = data.resolve_tags(search_tags, create=False)
    series = schema.table.series
    with schema.select("series", series.tags.contains(search_tags_ids)) as select:
        return select.all()


def get_data (args):
    for s in get_series(args):
        return DataFrameWrapper(selected)

            
def handle_nan (v):
    if np.isnan(v):
        return None
    else:
        return float(v)


class Data (Resource):
    """
    Data service REST interface.
    """

    def __init__ (self):
        super().__init__()
 
    def render_GET (self, request):
        series = schema.table.series
        print(request.args)
        with schema.select("series", series.id==int(request.args[b"id"][0].decode("utf-8"))) as select:
            with data.get_reader(select.all()[0]) as reader: 
                rows = reader._table.nrows
                from_index = max(0, int(request.args[b"start"][0].decode("utf-8"))) if b"start" in request.args else 0
                to_index = min(int(request.args[b"end"][0].decode("utf-8")), rows) if b"end" in request.args else rows
                from_time = int(request.args[b"starttime"][0].decode("utf-8")) if b"starttime" in request.args else None
                read_format = request.args[b"format"][0].decode("utf-8")
                print(read_format, from_time)

                if from_time:
                    diff = to_index - from_index
                    from_index = reader.nearest(from_time)
                    print(common.Time.time(from_time), from_index)
                    to_index = from_index + diff
                    print("adjusted from", from_index, to_index)

                if read_format == "ohlcv":
                    ohlc = []
                    volume = []
                    for row in reader.read(start=from_index, stop=to_index):
                        ohlc.append([float(row["time"]/1000000)
                          , handle_nan(row["open"])
                          , handle_nan(row["high"])
                          , handle_nan(row["low"])
                          , handle_nan(row["close"])
                          , handle_nan(row["volume"])])
                        volume.append([float(row["time"]/1000000), handle_nan(row["volume"])])
                    request.setHeader(b'Content-Type', b'text/json')
                    request.write(json.dumps({"ohlc": ohlc, "volume": volume, "adj": [], "from": int(from_index), "to": int(to_index), "max": int(rows)}).encode())
                elif read_format == "tick":
                    ticks = []
                    filtered = None
                    for row in reader.read_noindex(start=from_index, stop=to_index):
                        if filtered == None:
                            filtered = ("filter_class" in row.table.colnames)
                        if filtered:
                            ticks.append([float(row["time"]/1000000)
                              , handle_nan(row["price"])
                              , handle_nan(row["volume"])
                              , handle_nan(row["event"])
                              , row["qualifier"].decode("utf-8")
                              , handle_nan(row["acc_volume"])
                              , row["filter_class"].decode("utf-8")
                              , row["filter_detail"].decode("utf-8")
                              ])
                        else:
                            ticks.append([float(row["time"]/1000000)
                              , handle_nan(row["price"])
                              , handle_nan(row["volume"])
                              , handle_nan(row["event"])
                              , row["qualifier"].decode("utf-8")
                              , handle_nan(row["acc_volume"])
                              , ""
                              , ""
                              ])
                    request.setHeader(b'Content-Type', b'text/json')
                    request.write(json.dumps({"ticks": ticks, "from": int(from_index), "to": int(to_index), "max": int(rows)}).encode())
        print("done")
        return b""            

    def getChild (self, name, request):
        return self


class Spark (Resource):

    isLeaf = True

    def __init__ (self): 
        super().__init__()

    def getChild (self, name, request):
        return FourOhFour()

    def render_GET (self, request):
        params = {"id": int(request.args[b"id"][0].decode("utf-8")),
                  "width": request.args[b"width"][0].decode("utf-8"),
                  "height": request.args[b"height"][0].decode("utf-8")}
        series = schema.table.series
        with schema.select("series", series.id==params["id"]) as select:
            for s in select.all():

                cache_id = "spark_%s_%s_%s" % (params["id"], params["width"], params["height"])
                cached = memcache.get(cache_id)
                request.setHeader(b'Content-Type', b'image/png')
                if cached and cached["lm"] == s.last_modified:
                    request.write(cached["res"])
                else:
                    data = DataFrameWrapper(s)
                    plot = spark.Spark.plot(data=data.dframe["close"]
                            , volume=data.dframe["volume"]
                            , enable_volume=True
                            , width=float(request.args[b"width"][0])
                            , height=float(request.args[b"height"][0]))
                    cached = {"lm": s.last_modified, "res": plot.getvalue()}
                    memcache.set(cache_id, cached)
                    request.write(cached["res"])
                return b""

 
class Find (Resource):

    isLeaf = True

    def __init__ (self):
        super().__init__()

    def getChild (self, name, request):
        return FourOhFour()

    def render_GET (self, request):
        search_tags = json.loads(request.args[b"tags"][0].decode("utf-8"))
        search_tags_ids = data.resolve_tags(search_tags, create=False)
        series = schema.table.series
        series_list = []
        with schema.select("series", series.tags.contains(search_tags_ids)) as select:
            for s in select.all():
                series_list.append({"id":s.id, "symbol":s.symbol, "tags":data.decode_tags(s.tags)})
        request.setHeader(b'Content-Type', b'text/json')
        request.write(json.dumps(series_list).encode())
        return b""


class Summary (Resource):

    isLeaf = True

    def __init__ (self):
        super().__init__()

    def getChild (self, name, request):
        return FourOhFour()

    def render_GET (self, request):
        series = schema.table.series
        with schema.query("series", func.count(series.id), func.min(series.last_modified), func.max(series.last_modified)) as summary:
            for row in summary.all():
                result = dict(count=row[0], min=row[1], max=row[2])
                request.setHeader(b'Content-Type', b'text/json')
                request.write(json.dumps(result).encode())
        return b""


class Tag (Resource):

    isLeaf = True

    def __init__ (self):
        super().__init__()

    def getChild (self, name, request):
        return FourOhFour()

    def render_GET (self, request):
        tag = schema.table.tag
        if b"target" in request.args:
            target = request.args[b"target"][0].decode("utf-8")
            part = request.args[b"part"][0].decode("utf-8")
            values = []
            with schema.select("tag", tag.name==target, tag.value.like("%s%%" % (part))) as tag_values:
                for tag_value in tag_values.all():
                    values.append(tag_value.value)
                result = dict(values=sorted(values))
                request.setHeader(b'Content-Type', b'text/json')
                request.write(json.dumps(result).encode())
        else:
            with schema.query("tag", distinct(tag.name)) as tag_names:
                tags = []
                for tag_name in tag_names.all():
                    tags.append(tag_name[0])
                result = dict(tags=sorted(tags))
                request.setHeader(b'Content-Type', b'text/json')
                request.write(json.dumps(result).encode())
        return b""


class Api (Resource):

    isLeaf = False

    def __init__ (self):
        super().__init__()

    def getChild (self, name, request):
        if name == b"find":
            return Find()
        elif name == b"spark":
            return Spark()
        elif name == b"data":
            return Data()
        elif name == b"summary":
            return Summary()
        elif name == b"tag":
            return Tag()
        return FourOhFour()

class Index (Resource):

    isLeaf = False

    def __init__ (self):
        super().__init__()

    def getChild (self, name, request):
        if name == b"api":
            return Api()
        return FourOhFour()

    def render_GET (self, request):
         return b""

class FourOhFour (Resource):

    isLeaf = True
 
    def __init__ (self):
        super().__init__()

    def getChild (self, name, request):
        return self

    def render_GET (self, request):
        request.setResponseCode(404)
        request.finish()
        return NOT_DONE_YET

res = Index()
factory = Site(res)
reactor.listenTCP(3010, factory)
reactor.run()


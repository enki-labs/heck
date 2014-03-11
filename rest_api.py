
import argparse
import json
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
from sqlalchemy.sql import and_
import celery
import redis
from datetime import timedelta
from babel.dates import format_timedelta

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


class Data (Resource):
    """
    Data service REST interface.
    """

    def __init__ (self):
        super().__init__()
 
    def render_GET (self, request):
        series = schema.table.series

        response = dict(data=[], actual=[], volume=[], adj=[], tstart=0, tend=0, istart=0, iend=0)

        with schema.select("series", series.id==int(request.args[b"id"][0].decode("utf-8"))) as select:
            with data.get_reader(select.first()) as reader: 
                rows = reader.count()
                from_index = max(0, int(request.args[b"start"][0].decode("utf-8"))) if b"start" in request.args else 0
                to_index = min(int(request.args[b"end"][0].decode("utf-8")), rows) if b"end" in request.args else rows
                from_time = int(request.args[b"starttime"][0].decode("utf-8")) if b"starttime" in request.args else None
                read_format = request.args[b"format"][0].decode("utf-8")

                if from_time:
                    diff = to_index - from_index
                    from_index = reader.nearest(from_time)
                    to_index = from_index + diff

                if read_format == "ohlcv":
                    for row in reader.read(start=from_index, stop=to_index):
                        response["data"].append([float(row["time"]/1000000)
                          , handle_nan(row["open"])
                          , handle_nan(row["high"])
                          , handle_nan(row["low"])
                          , handle_nan(row["close"])
                          , handle_nan(row["volume"])])
                        response["actual"].append([float(row["time"]/1000000), handle_nan(row["actual"])])
                        response["volume"].append([float(row["time"]/1000000), handle_nan(row["volume"])])

                    if len(response["data"]) > 0:
                        response["tstart"] = response["data"][0][0]
                        response["tend"] = response["data"][-1][0] 
                        response["istart"] = int(from_index)
                        response["iend"] = int(to_index)
 
                elif read_format == "tick":
                    filtered = None
                    common.log.info("read tick %s to %s" % (from_index, to_index))
                    for row in reader.read_noindex(start=from_index, stop=to_index):
                        if filtered == None:
                            filtered = ("filter_class" in row.table.colnames)
                        if filtered:
                            response["data"].append([float(row["time"]/1000000)
                              , handle_nan(row["price"])
                              , handle_nan(row["volume"])
                              , handle_nan(row["event"])
                              , row["qualifier"].decode("utf-8")
                              , handle_nan(row["acc_volume"])
                              , row["filter_class"].decode("utf-8")
                              , row["filter_detail"].decode("utf-8")
                              ])
                        else:
                            response["data"].append([float(row["time"]/1000000)
                              , handle_nan(row["price"])
                              , handle_nan(row["volume"])
                              , handle_nan(row["event"])
                              , row["qualifier"].decode("utf-8")
                              , handle_nan(row["acc_volume"])
                              , ""
                              , ""
                              ])
                        response["volume"].append([float(row["time"]/1000000), handle_nan(row["volume"])])
                    if len(response["data"]) > 0:
                        response["tstart"] = response["data"][0][0]
                        response["tend"] = response["data"][-1][0]
                        response["istart"] = int(from_index)
                        response["iend"] = int(to_index)
        
        common.log.info("write result")
        request.setHeader(b'Content-Type', b'text/json')
        request.write(json.dumps(response).encode())
        common.log.info("write complete")
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
                  "height": request.args[b"height"][0].decode("utf-8"),
                  "format": request.args[b"format"][0].decode("utf-8"),
                  "resolution": int(request.args[b"resolution"][0].decode("utf-8"))
                 }

        series = schema.table.series
        with schema.select("series", series.id==params["id"]) as select:
            for s in select.all():

                cache_id = "spark_%s_%s_%s_%s_%s" % (params["id"], params["width"], params["height"], params["format"], params["resolution"])
                cached = memcache.get(cache_id)
                request.setHeader(b'Content-Type', b'image/png')
                if cached and cached["lm"] == s.last_modified:
                    request.write(cached["res"])
                else:
                    if params["format"] == "ohlcv":
                        data = DataFrameWrapper(s, resolution=params["resolution"])
                        if len(data.dframe) > 0 and "close" in data.dframe.columns.values:
                            plot = spark.Spark.plot(data=data.dframe["close"]
                                    , volume=data.dframe["volume"]
                                    , enable_volume=True
                                    , width=float(request.args[b"width"][0])
                                    , height=float(request.args[b"height"][0]))
                            cached = {"lm": s.last_modified, "res": plot.getvalue()}
                            memcache.set(cache_id, cached)
                            request.write(cached["res"])
                        else:
                            #request.write()
                            pass
                    elif params["format"] == "tick":
                        data = DataFrameWrapper(s, no_index=True, resolution=params["resolution"])
                        plot = spark.Spark.plot(data=data.dframe["price"]
                                , volume=0
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
            for s in select.limit(100).all():
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

        if b"id" in request.args:
            response = dict(summary=[], tzero=0, tmax=0, imax=0)

            with schema.select("series", series.id==int(request.args[b"id"][0].decode("utf-8"))) as select:
                with data.get_reader(select.first()) as reader:
                    rows = reader.count()
                    read_format = request.args[b"format"][0].decode("utf-8")
                    resolution = int(request.args[b"resolution"][0].decode("utf-8"))

                    from_index = 0
                    to_index = rows
                    step = 1
                    if to_index > resolution:
                        step = int(to_index / resolution)

                    if read_format == "ohlcv":
                        for row in reader.read(start=from_index, stop=to_index, step=step):
                            response["summary"].append([float(row["time"]/1000000)
                                , handle_nan(row["open"])])
                        if rows > 0:
                            for row in reader.read(start=0, stop=1):
                                response["tzero"] = float(row["time"]/1000000)
                            for row in reader.read(start=rows-1, stop=rows):
                                response["tmax"] = float(row["time"]/1000000)
                    elif read_format == "tick":
                        for row in reader.read_noindex(start=from_index, stop=to_index, step=step):
                            response["summary"].append([float(row["time"]/1000000)
                                , handle_nan(row["price"])])
                        if rows > 0:
                            for row in reader.read_noindex(start=0, stop=1):
                                response["tzero"] = float(row["time"]/1000000)
                            for row in reader.read_noindex(start=rows-1, stop=rows):
                                response["tmax"] = float(row["time"]/1000000)

                    response["imax"] = int(rows)
                    request.setHeader(b'Content-Type', b'text/json')
                    request.write(json.dumps(response).encode())

        else:
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


class RabbitmqAdmin (object):
    def __init__ (self, args):
        self._auth = (args.celery_user, args.celery_pass)
        self._api = "http://%s:55672/api" % args.celery_host

    def queues (self):
        resp = requests.get("%s/queues/task" % self._api, auth=self._auth)
        result = []
        for queue in resp.json():
            if queue["name"] == "celery":
                result.append({"name": queue["node"],
                               "messages": queue["messages"]})
        return result

class Task (Resource):

    isLeaf = True

    def __init__ (self, args):
        super().__init__()
        #import celery
        #self._celery = celery.Celery()
        #config = {"BROKER_URL": args.celery_broker,
        #          "CELERY_RESULT_BACKEND": "database",
        #          "CELERY_RESULT_DBURI": args.celery_backend,
        #          "CELERY_RESULT_DB_TABLENAMES": args.celery_tables}
        #self._celery.config_from_object(config)
        #self._amqp_api = RabbitmqAdmin(args) 

    def getChild (self, name, request):
        return FourOhFour()

    def render_GET (self, request):

        action = request.args[b"action"][0].decode("utf-8")
        result = {}
        
        if action == "active":        
            #executing = self._celery.control.inspect().active()
            #queues = self._amqp_api.queues()
            queues = celery_client.control.inspect().reserved()
            if queues:
                queues["unassigned"] = redis_client.llen("celery")
                queues["control"] = redis_client.llen("control")
            else:
                queues = dict(unassigned=redis_client.llen("celery"), control=redis_client.llen("control"))

            executing = {}
            with schema.query("status", schema.table.status.node, func.count(schema.table.status.id)) as items:
                for row in items.filter(schema.table.status.status=="PROGRESS").group_by(schema.table.status.node).all():
                    print(row)
                    executing[row[0]] = row[1]
            result = {"executing": executing,
                      "stats": celery_client.control.inspect().stats(),
                      "queues": queues}
            
        if action in ["working", "failed"]:
            result = []
            with schema.select("status") as items:
                if action == "failed":
                    items = items.filter(schema.table.status.status=="FAILURE")
                for item in items.order_by(desc(schema.table.status.last_modified)).order_by(desc(schema.table.status.started)).limit(100).all():
                    progress = 0
                    per_second = 0
                    estimate = ""
                    item_result = json.loads(item.result)
                    if item.status == "PROGRESS":
                        progress = int(item_result["step"]/item_result["total"]*100)
                        per_second = item_result["per_second"]
                        est_seconds = (item_result["total"] - item_result["step"])/item_result["per_second"]
                        estimate = format_timedelta(timedelta(seconds=est_seconds), locale="en_US")
                    result.append({"node": item.node, "task_id": item.task_id, "name": item.name, "status": item.status, "progress": progress, "per_second": per_second, "estimate": estimate, "time_start": item.started, "time_end": item.last_modified, "result": item_result}) 

        elif action == "list":
            item = request.args[b"item"][0].decode("utf-8")
            item_filter = json.loads(request.args[b"filter"][0].decode("utf-8"))
            if item == "config":
                result = []
                tags = data.resolve_tags(item_filter, create=False)
                with schema.select("config", schema.table.config.tags.contains(tags)) as items:
                    for row in items.limit(100).all():
                        print(row.tags)
                        rowdict = row.to_dict()
                        rowdict["content"] = json.loads(rowdict["content"])
                        rowdict["tags"] = data.decode_tags(rowdict["tags"])
                        result.append(rowdict)
            elif item == "process":
                result = []
                with schema.select(item, schema.table.process.name.ilike("%%%s%%" % item_filter["name"]), 
                                         schema.table.process.processor.ilike("%%%s%%" % item_filter["processor"])) as items:
                    for row in items.order_by(schema.table.process.name).limit(100).all():
                        result.append(row.to_dict())
            elif item == "symbol":
                result = []
                with schema.select(item, schema.table.symbol.symbol.ilike("%%%s%%" % item_filter["symbol"]),
                                         schema.table.symbol.category.ilike("%%%s%%" % item_filter["category"]),
                                         schema.table.symbol.name.ilike("%%%s%%" % item_filter["name"])) as items:
                    for row in items.order_by(schema.table.symbol.symbol).limit(100).all():
                        result.append(row.to_dict())
            elif item == "symbol_resolve":
                result = []
                with schema.select(item, schema.table.symbol_resolve.symbol.ilike("%%%s%%" % item_filter["symbol"]),
                                         schema.table.symbol_resolve.source.ilike("%%%s%%" % item_filter["source"]),
                                         schema.table.symbol_resolve.resolve.ilike("%%%s%%" % item_filter["resolve"])) as items:
                    for row in items.order_by(schema.table.symbol.symbol).limit(100).all():
                        result.append(row.to_dict())

        elif action == "delete":
            item = request.args[b"item"][0].decode("utf-8")
            target = json.loads(request.args[b"target"][0].decode("utf-8"))
            item_schema = None
            if item == "config":
                item_schema = schema.table.config
            elif item == "process":
                item_schema = schema.table.process
            elif item == "symbol": #TODO add id
                symbols = []
                for target_symbol in target:
                    symbols.append(target_item["symbol"])
                schema.delete_query("symbol", schema.table.symbol.in_(symbols))
            elif item == "symbol_resolve": #TODO add id
                for target_item in target:
                    schema.delete_query("symbol_resolve", and_(schema.table.symbol_resolve.symbol==target_item["symbol"], schema.table.symbol_resolve.source==target_item["source"]))
            if item_schema:
                ids = []
                for target_item in target:
                    ids.append(target_item["id"])
                schema.delete_query(item, item_schema.id.in_(ids))
        elif action == "save":
            item = request.args[b"item"][0].decode("utf-8")
            target = json.loads(request.args[b"target"][0].decode("utf-8"))
            if item == "config":
                for target_item in target:
                    target_item["tags"] = data.resolve_tags(target_item["tags"], create=True)
                    target_item["content"] = json.dumps(target_item["content"])
                    target_item["last_modified"] = common.Time.tick()
                    if target_item["id"]: #TODO how to upsert with SQLAlchemy if id set by init?
                        get_item = schema.select_one("config", schema.table.config.id==target_item["id"])
                        get_item.tags = target_item["tags"]
                        get_item.content = target_item["content"]
                        get_item.last_modified = target_item["last_modified"]
                        schema.save(get_item)
                    else:
                        schema.save(schema.table.config.from_dict(target_item))
            elif item == "process":
                for target_item in target:
                    print(target_item)
                    target_item["last_modified"] = common.Time.tick()
                    if target_item["id"]: #TODO how to upsert with SQLAlchemy if id set by init?
                        get_item = schema.select_one("process", schema.table.process.id==target_item["id"])
                        get_item.name = target_item["name"]
                        get_item.processor = target_item["processor"]
                        #TODO all tables should support json to/from
                        get_item.search = json.dumps(target_item["search"])
                        get_item.output = json.dumps(target_item["output"])
                        get_item.last_modified = target_item["last_modified"]
                        schema.save(get_item)
                    else:
                        schema.save(schema.table.process.from_dict(target_item))
            elif item == "symbol":
                for target_item in target:
                    existing = schema.select_one("symbol", schema.table.symbol.symbol==target_item["symbol"])
                    target_item["last_modified"] = common.Time.tick()
                    if existing:
                        existing.category = target_item["category"]
                        existing.meta = json.dumps(target_item["meta"])
                        existing.last_modified = target_item["last_modified"]
                        schema.save(existing)
                    else:
                        schema.save(schema.table.symbol.from_dict(target_item))
            elif item == "symbol_resolve":
                for target_item in target:
                    target_item["last_modified"] = common.Time.tick()
                    existing = schema.select_one("symbol_resove", and_(schema.table.symbol_resolve.symbol==target_item["symbol"], schema.table.symbol_resolve.source==target_item["source"]))
                    if existing:
                        existing.resolve = target_item["resolve"]
                        existing.adjust = json.dumps(target_item["adjust"])
                        existing.last_modified = target_item["last_modified"]
                        schema.save(existing)
                    else:
                        schema.save(schema.table.symbol_resolve.from_dict(target_item))
        elif action == "progress":
            result["task"] = []
            #TODO does the Celery API support listing tasks in DB backend?
            with schema.query("celery_task", "task_id") as tasks:
                for task in tasks.from_statement("select task_id from celery_task").limit(100).all():
                    task_info = self._celery.backend.get_task_meta(task[0])
                    task_info["date_done"] = common.Time.tick(task_info["date_done"])
                    result["task"].append(task_info)
        
        request.setHeader(b'Content-Type', b'text/json')
        request.write(json.dumps(result).encode())
        import sys
        sys.stdout.flush()
        return b""

class Api (Resource):

    isLeaf = False

    def __init__ (self, args):
        super().__init__()
        self._args = args

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
        elif name == b"task":
            return Task(args)
        return FourOhFour()

class Index (Resource):

    isLeaf = False

    def __init__ (self, args):
        super().__init__()
        self._args = args

    def getChild (self, name, request):
        if name == b"api":
            return Api(self._args)
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

parser = argparse.ArgumentParser(description="REST API")
parser.add_argument("--port", type=int, help="API port (eg: 3010)", required=True)
parser.add_argument("--celery_user", help="Celery broker user", required=True)
parser.add_argument("--celery_pass", help="Celery broker password", required=True)
parser.add_argument("--celery_host", help="Celery broker host (eg: localhost)", required=True)
parser.add_argument("--celery_broker", help="Celery broker URL (eg: amqp://celery:pass@localhost:5672/task)", required=True)
parser.add_argument("--celery_backend", help="Celery DB URL (eg: db+postgresql://heck:pass@localhost/heck)", required=True)
parser.add_argument("--celery_tables", type=json.loads, help='Celery DB table names (eg: {"task": "celery_task", "group": "celery_group"})', required=True) 
args = parser.parse_args()

celery_client = celery.Celery()
print(args.celery_broker)
celery_client.config_from_object({"BROKER_URL": args.celery_broker,
                  "CELERY_RESULT_BACKEND": "database",
                  "CELERY_RESULT_DBURI": args.celery_backend,
                  "CELERY_RESULT_DB_TABLENAMES": args.celery_tables})

memcache = memcache.Client(['127.0.0.1:11211'], debug=0)
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, password=args.celery_pass)

res = Index(args)
factory = Site(res)
reactor.listenTCP(args.port, factory)
reactor.run()



import json
from twisted.internet import reactor
from twisted.web.server import Site
from twisted.web.resource import Resource

from config import prod
from lib import schema
from lib import data
from lib.process import DataFrameWrapper
from lib.data.spark import Spark


class DataService (Resource):
    """
    Data service REST interface.
    """

    def __init__ (self):
        super().__init__()
 
    def render_GET (self, request):
        if b"output" not in request.args:
            print(request.args)
            request.setHeader(b'Content-Type', b'text/json')
            ret = json.dumps(dict(error="Missing output parameter")).encode()
            request.write(ret)
            return b""

        output = request.args[b"output"][0]

        if output == b"spark":
            
            search_tags = json.loads(request.args[b"tags"][0].decode("utf-8"))
            symbol = request.args[b"symbol"][0].decode("utf-8")
            search_tags_ids = data.resolve_tags(search_tags, create=False)
            series = schema.table.series
            with schema.select("series", series.symbol==symbol, series.tags.contains(search_tags_ids)) as select:
                for selected in select.all():
                    df = DataFrameWrapper(selected)
                    print(df.dframe["volume"])
                    plot = Spark.plot(data=df.dframe["close"], volume=df.dframe["volume"], enable_volume=True, width=float(request.args[b"width"][0]), height=float(request.args[b"height"][0]))
                    request.setHeader(b'Content-Type', b'image/png')
                    request.write(plot.getvalue())
                    return b""
        else:
            request.setHeader('Content-Type', 'text/json')
            request.write(json.dumps(dict(error="Unknown output type %s" % (output))))
            return b""
        

    def getChild (self, name, request):
        return self

res = DataService()
factory = Site(res)
reactor.listenTCP(3010, factory)
reactor.run()


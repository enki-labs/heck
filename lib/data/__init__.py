"""
Global methods for data stores.

"""

from autologging import logged, traced, TracedMethods
import time
from lib import common
from lib import schema
from tables import *
from lib.data import ohlc

@traced(common.log)
def resolve_tags (tags, create):
    """
    Resolve tags to database ids and create if required.
    """
    tagids = []

    for name, value in tags.items():
        tag = schema.select_one("tag", name=name, value=value)
        if not tag and not create:
            raise Exception("Tag %s (%s) does not exist" % (name, value))
        elif not tag:
            tag = schema.table("tag")
            tag.name = name
            tag.value = value
            schema.save(tag)
        tagids.append(tag.id)

    return sorted(tagids) 


@traced(common.log)
def get_series (tags, create):
    """
    Open a store and create if required.
    """

    if "format" not in tags:
        raise Exception("Format is a required series tag")

    tagids = resolve_tags(tags, create)
    
    series = schema.select_one("series", tags=tagids)
    if not series and not create:
        raise Exception("Series (%s) (%s) does not exist" % (tags, tagids))
    elif not series:
        series = schema.table("series")
        series.tags = tagids
        schema.save(series)

    if tags["format"] == "ohlc":
        filters = Filters(complevel = 9, complib = "blosc", fletcher32 = False)
        return ohlc.Ohlc(common.Path.get("series_store", tags), filters)
    else:
        raise Exception("Unknown series format %s" % (tags["format"]))


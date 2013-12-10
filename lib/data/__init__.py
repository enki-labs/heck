"""
Global methods for data stores.

"""

import time
from lib import common
from lib import schema
from tables import *

def decode_tags (tag_ids):
    """
    Decode tag ids to strings.
    """
    tags = dict()
    for tag_id in tag_ids:
        tag = schema.select_one("tag", schema.table.tag.id==tag_id)
        if not tag:
            raise Exception("Tag id %s does not exist" % (tag_id))
        tags[tag.name] = tag.value
    return tags    

def resolve_tags (tags, create):
    """
    Resolve tags to database ids and create if required.
    """
    tagids = []

    for name, value in tags.items():
        tag = schema.select_one("tag", schema.table.tag.name==name, schema.table.tag.value==value)
        if not tag and not create:
            raise Exception("Tag %s (%s) does not exist" % (name, value))
        elif not tag:
            tag = schema.table.tag()
            tag.name = name
            tag.value = value
            schema.save(tag)
        tagids.append(tag.id)

    return sorted(tagids) 

def get_reader (series):
    """
    Open a store.
    """
    from lib.data import ohlc
    tags = decode_tags(series.tags)
    if tags["format"] == "ohlc":
        return ohlc.OhlcReader(series)
    else:
        raise Exception("Unknown series format %s" % (tags["format"]))

def get_writer (tags, first, last, create=True, overwrite=False, append=False):
    """
    Open a store and create if required.
    """

    from lib.data import ohlc
    from datetime import datetime

    if "format" not in tags:
        raise Exception("Format is a required series tag")

    if "symbol" not in tags:
        raise Exception("Symbol is a required series tag")

    first_tick = common.Time.tick(first) if isinstance(first, datetime) else first
    last_tick = common.Time.tick(last) if isinstance(last, datetime) else last

    tagids = resolve_tags(tags, create)    
    series = schema.select_one("series", schema.table.series.symbol==tags["symbol"], schema.table.series.tags==tagids)
    if not series and not create:
        raise Exception("Series (%s) (%s) does not exist" % (tags, tagids))
    elif not series:
        series = schema.table.series()
        series.symbol = tags["symbol"]
        series.tags = tagids
        schema.save(series)

    if tags["format"] == "ohlc":
        common.log.info("opening OHLC file for writing")
        filters = Filters(complevel = 9, complib = "blosc", fletcher32 = False)
        return ohlc.OhlcWriter(series, filters, first_tick, last_tick, overwrite=overwrite, append=append)
    else:
        raise Exception("Unknown series format %s" % (tags["format"]))

def update_series (series, count, tick_start, tick_end):
    """
    Update cached series info.
    """
    series.count = count
    series.start = tick_start
    series.end = tick_end
    series.last_modified = common.Time.tick()
    schema.save(series)
    

class OverlapException (Exception):
    """ Data insertion would overlap existing data """
    pass


class Reader (object):
    """
    Base class for data store readers.
    """

    def __init__ (self, series, path_type):
        self._series = series
        self._local = common.store.read(common.Path.resolve_path(path_type, series)).__enter__()
        self._file = openFile(self._local.local().name, mode="r", title="")
        self._table = self._file.root.data

    def read (self, start=None, stop=None):
        """
        Read data time ordered.
        """
        return self._table.read_sorted(self._table.cols.time, start=start, stop=stop)

    def read_iter (self, start=None, stop=None):
        """
        Read data time ordered using an iterator.
        """
        return self._table.itersorted(self._table.cols.time, start=start, stop=stop)

    def close (self):
        """
        Close underlying file.
        """
        self._table.close()
        self._file.close()
        self._local.__exit__(None, None, None)


class Writer (object):
    """
    Base class for data store writers.
    """

    def _check_overlap (self, first, last, overwrite):
        """
        Check for overlap between existing and new data.
        """
        rowcount = self._table.nrows
        if rowcount == 0: return

        first_row = self._table.read_sorted(self._table.cols.time, start=0, stop=1)[0]
        last_row = self._table.read_sorted(self._table.cols.time, start=rowcount-1)[0]
        
        if first < last_row["time"] and last > first_row["time"]:
            #find overlapping region
            for row in self._table.itersorted(self._table.cols.time):
                if row["time"] > first:
                    if not overwrite:
                        if last > row["time"]:
                            raise OverlapException()
                        else:
                            break
                    else:
                        pass #TODO: copy to new table, filter overlap


    def close (self):
        """
        Close the file and flush data.
        """
        self._table.flush()
        self._table.flush_rows_to_index()
        self._table.cols.time.reindex_dirty()
        count = int(self._table.nrows)
        start = -1 if count == 0 else int(self._table[0]['time'])
        end = -1 if count == 0 else int(self._table[-1]['time'])
        # close and update 
        self._file.flush()
        self._file.close()
        self._local.save()
        self._local.__exit__(None, None, None)
        # write to db
        update_series(self._series, count, start, end)


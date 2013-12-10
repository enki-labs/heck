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

    def __init__ (self, series, filters, first, last, overwrite, append, path_name, description):
        """
        Constructor.
        """
        self._series = series
        self._filters = filters
        self._first = first
        self._last = last
        self._overwrite = overwrite
        self._append = append
        self._store = None
        self._file = None
        self._table = None
        self._path_name = path_name
        self._description = description

    def _get_row (self, index):
        """
        Get the given row by index and time sort.
        """
        rowcount = self._table.nrows
        if rowcount == 0: return None

        if index >= 0:
            return self._table.read_sorted(self._table.cols.time, start=index, stop=index+1)[0]
        else:
            return self._table.read_sorted(self._table.cols.time, start=(rowcount + index), stop=(rowcount + index + 1))[0]
 
    def _check_overlap (self):
        """
        Check for overlap between existing and new data.
        """
        rowcount = self._table.nrows
        if rowcount == 0: return

        first_row = self._get_row(0)
        last_row = self._get_row(-1)
        
        if self._first < last_row["time"] and self._last > first_row["time"]:
            #find overlapping region
            for row in self._table.itersorted(self._table.cols.time):
                if row["time"] > self._first:
                    if not overwrite:
                        if self._last > row["time"]:
                            raise OverlapException()
                        else:
                            break
                    else:
                        pass #TODO: copy to new table, filter overlap


    def save (self):
        """
        Save the file and flush data.
        """
        self._table.flush()
        self._table.flush_rows_to_index()
        self._table.cols.time.reindex()
        self._table.flush()
        count = int(self._table.nrows)
        start = -1 if count == 0 else int(self._get_row(0)['time'])
        end = -1 if count == 0 else int(self._get_row(-1)['time'])
        self._file.flush()
        self._store.save()
        update_series(self._series, count, start, end)

    def __enter__ (self):
        """
        Init for 'with' block.
        """
        if self._append:
            self._store = common.store.append(
                          common.Path.resolve_path(self._path_name, self._series)
                          , open_handle=False).__enter__()
        else:
            self._store = common.store.write(
                          common.Path.resolve_path(self._path_name, self._series)
                          , open_handle=False).__enter__()
        
        try:
            self._file = open_file(self._store.local_path(), mode=("a" if self._append else "w"), title="")
            if "data" in self._file.root:
                self._table = self._file.root.data
                self._check_overlap()
            else:
                self._table = self._file.createTable(self._file.root, 'data', self._description, "data", filters=self._filters)
                self._table.cols.time.create_csindex()
                self._table.autoindex = False
        except Exception:
            self.__exit__(None, None, None) 
            raise
        
        return self

    def __exit__ (self, typ, value, tb):
        """
        Handle 'with' exit.
        """
        if self._file:
            self._file.close()
        if self._store:
            self._store.__exit__(None, None, None)


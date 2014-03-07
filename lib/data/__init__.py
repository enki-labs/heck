"""
Global methods for data stores.

"""

import time
import exception
import math
from tables import *
import pandas as pd
from bisect import bisect_left
from lib import common
from lib import schema


def decode_tags (tag_ids):
    """
    Decode tag ids to strings.
    """
    tags = dict()
    for tag_id in tag_ids:
        tag = schema.select_one("tag", schema.table.tag.id==tag_id)
        if not tag:
            raise exception.MissingTagException("Tag id %s does not exist" % (tag_id))
        tags[tag.name] = tag.value
    return tags    


def resolve_tags (tags, create):
    """
    Resolve tags to database ids and create if required.
    """
    tagids = []

    for name, value in tags.items():
        if value == "*": #return any with name
            with schema.select("tag", schema.table.tag.name==name) as select:
                for selected in select.all():
                    tagids.append(selected.id) 
        else:
            tag = schema.select_one("tag", schema.table.tag.name==name, schema.table.tag.value==value)
            if not tag and not create:
                raise exception.MissingTagException("Tag %s (%s) does not exist" % (name, value))
            elif not tag:
                tag = schema.table.tag()
                tag.name = name
                tag.value = value
                schema.save(tag)
            tagids.append(tag.id)

    return sorted(tagids) 


class PandasReader (object):

    def __init__ (self, series, path_type):
        self._series = series
        self._path_type = path_type
        self._store = None
        self._dataframe = None

    @property
    def df (self):
        return self._dataframe

    def __enter__ (self):
        """
        Open file and return reader.
        """
        self._store = common.store.read(common.Path.resolve_path(self._path_type, self._series), open_handle=False).__enter__()
        self._dataframe = pd.read_hdf(self._store.local_path(), key="data")
        self._dataframe = self._dataframe.set_index(pd.DatetimeIndex(pd.Series(self._dataframe["time"]).astype("datetime64[ns]"), tz="UTC"))
        return self

    def __exit__ (self, typ, value, tb):
        """
        Close underlying file.
        """
        if self._store: self._store.__exit__(None, None, None)


def get_reader_pandas (series):
    """
    Open a store.
    """
    tags = decode_tags(series.tags)
    if tags["format"] == "ohlc":
        return PandasReader(series, "series_ohlc")
    elif tags["format"] == "tick":
        return PandasReader(series, "series_tick")
    else:
        raise Exception("Unknown series format %s" % (tags["format"]))


def get_reader (series):
    """
    Open a store.
    """
    from lib.data import ohlc
    from lib.data import tick
    tags = decode_tags(series.tags)
    if tags["format"] == "ohlc":
        return ohlc.OhlcReader(series)
    elif tags["format"] == "tick":
        return tick.TickReader(series)
    else:
        raise Exception("Unknown series format %s" % (tags["format"]))


def get_writer_lock (tags, create=True, overwrite=False, append=False):
    """
    Open a store wrapper and create series if required.
    """

    from lib.data import ohlc
    from datetime import datetime

    if "format" not in tags:
        raise Exception("Format is a required series tag")

    if "symbol" not in tags:
        raise Exception("Symbol is a required series tag")

    if tags["format"] not in ["ohlc", "tick"]:
        raise Exception("Unknown series format %s" % (tags["format"]))

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
        return ohlc.OhlcWriterLock(series, filters, overwrite=overwrite, append=append)
    elif tags["format"] == "tick":
        filters = Filters(complevel = 9, complib = "blosc", fletcher32 = False)
        return tick.TickWriterLock(series, filters, overwrite=overwrite, append=append)
    else:
        raise Exception("Unknown series format %s" % (tags["format"]))
    

def get_writer (tags, first, last, create=True, overwrite=False, append=False):
    """
    Open a store and create if required.
    """

    from lib.data import ohlc
    from lib.data import tick
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
    elif tags["format"] == "tick":
        filters = Filters(complevel = 9, complib = "blosc", fletcher32 = False)
        return tick.TickWriter(series, filters, first_tick, last_tick, overwrite=overwrite, append=append)
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
    

class Reader (object):
    """
    Base class for data store readers.
    """

    def __init__ (self, series, path_type):
        self._series = series
        self._path_type = path_type
        self._store = None
        self._file = None
        self._table = None

    def nearest (self, timet):
        """
        Find nearest index to a given time.
        """
        def next_index (low, high):
            return math.floor(low+((high-low)/2)) 

        def search_up (table, low, high, timet):
            search = next_index(low, high)
            searcht = table[search]["time"]
            while (high - low) > 1 and searcht < timet:
                low = search
                search = next_index(low, high)
                searcht = table[search]["time"]
            if searcht == timet:
                return (search, search)
            else:
                return (low, search)

        def search_down (table, low, high, timet):
            search = next_index(low, high)
            searcht = table[search]["time"]
            while (high - low) > 1 and searcht > timet:
                high = search
                search = next_index(low, high)
                searcht = table[search]["time"]
            if searcht == timet:
                return (search, search)
            else:
                return (search, high)

        low = 0
        high = self._table.nrows
        while low != high:
            low,  high = search_up(self._table, low, high, timet)
            if low != high:
                low, high = search_down(self._table, low, high, timet)
        return low

    def __enter__ (self):
        """
        Open file and return reader.
        """
        self._store = common.store.read(common.Path.resolve_path(self._path_type, self._series), open_handle=False).__enter__()
        self._file = openFile(self._store.local_path(), mode="r", title="")
        self._table = self._file.root.data
        return self

    def read_noindex (self, start=None, stop=None, step=1):
        """
        Read data from a sorted file.
        """
        if start == None and stop == None:
            start = 0
            stop = self._table.nrows

        return self._table.iterrows(start=start, stop=stop, step=step)

    def read (self, start=None, stop=None, step=1, no_index=False):
        """
        Read data time ordered.
        """
        if start == None and stop == None:
            start = 0
            stop = self._table.nrows
        if no_index:
            return self._table.read(start=start, stop=stop, step=step)
        else:
            return self._table.read_sorted(self._table.cols.time, start=start, stop=stop, step=step, checkCSI=True)

    def read_iter (self, start=None, stop=None, step=1):
        """
        Read data time ordered using an iterator.
        """
        if start == None and stop == None:
            start = 0
            stop = self._table.nrows
        return self._table.itersorted(self._table.cols.time, start=start, stop=stop, step=step)

    def __exit__ (self, typ, value, tb):
        """
        Close underlying file.
        """
        if self._table: self._table.close()
        if self._file: self._file.close()
        if self._store: self._store.__exit__(None, None, None)


class InnerWriter (object):

    def __init__ (self, writer_lock, first=None, last=None):
        self._writer_lock = writer_lock
        if first and last:
            self._check_overlap(first, last)


    def save (self, table_only=False):
        self._writer_lock.save(table_only)


    def _get_row (self, index):
        """
        Get the given row by index and time sort.
        """
        rowcount = self._writer_lock._table.nrows
        if rowcount == 0: return None

        if index >= 0:
            return self._writer_lock._table.read_sorted(self._writer_lock._table.cols.time, start=index, stop=index+1)[0]
        else:
            return self._writer_lock._table.read_sorted(self._writer_lock._table.cols.time, start=(rowcount + index), stop=(rowcount + index + 1))[0]


    def _check_overlap (self, first, last):
        """
        Check for overlap between existing and new data.
        """
        rowcount = self._writer_lock._table.nrows
        if rowcount == 0: return

        first_row = self._get_row(0)
        last_row = self._get_row(-1)

        if first < last_row["time"] and last > first_row["time"]:
            #find overlapping region
            for row in self._writer_lock._table.itersorted(self._writer_lock._table.cols.time):
                if row["time"] > first:
                    if not self._writer_lock._overwrite:
                        if last > row["time"]:
                            raise exception.OverlapException()
                        else:
                            break
                    else:
                        pass #TODO: copy to new table, filter overlap

    
class WriterLock (object):
    """
    Base class for data store writers with early locks.
    """

    def __init__ (self, series, filters, overwrite, append, path_name, description):
        """
        Constructor.
        """
        self._series = series
        self._filters = filters
        self._overwrite = overwrite
        self._append = append
        self._store = None
        self._file = None
        self._table = None
        self._path_name = path_name
        self._description = description


    def writer (self, first=None, last=None):
        """
        Return a writer instance for this lock.
        """
        raise NotImplementedError()


    def save (self, table_only=False):
        """
        Save the file and flush data.
        """
        self._table.flush()
        self._table.flush_rows_to_index()
        self._table.cols.time.reindex_dirty()
        self._table.flush()
        if not table_only:
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


    def _check_overlap (self, first, last):
        """
        Check for overlap between existing and new data.
        """
        rowcount = self._table.nrows
        if rowcount == 0: return

        first_row = self._get_row(0)
        last_row = self._get_row(-1)

        if first < last_row["time"] and last > first_row["time"]:
            #find overlapping region
            for row in self._table.itersorted(self._table.cols.time):
                if row["time"] > first:
                    if not self._overwrite:
                        if last > row["time"]:
                            raise exception.OverlapException()
                        else:
                            break
                    else:
                        pass #TODO: copy to new table, filter overlap


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
                    if not self._overwrite:
                        if self._last > row["time"]:
                            raise exception.OverlapException()
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
        self._table.cols.time.reindex_dirty()
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


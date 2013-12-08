"""
Data class - open, high, low, close, volume and open interest.

"""

from autologging import logged, traced, TracedMethods
import time
from lib import common
from lib import schema
from lib.data import update_series, Writer, Reader
from tables import *


class OhlcDescription (IsDescription):
    """
    OHLC data table description.
    """
    time            = Int64Col()
    open            = Float64Col()
    high            = Float64Col()
    low             = Float64Col()
    close           = Float64Col()
    volume          = Float64Col()
    openInterest    = Float64Col()
    actual          = Float64Col()


class OhlcReader (Reader):
    """
    Read an OHLC data store.
    """

    def __init__ (self, series):
        super(OhlcReader, self).__init__(series, "series_ohlc")


class OhlcWriter (Writer):
    """
    A writable OHLC data store.
    """
    
    def __init__ (self, series, filters, first, last, overwrite, append=False):
        self._series = series
        self._local = common.store.append(common.Path.resolve_path("series_ohlc", series)).__enter__()
        self._file = open_file(self._local.local().name, mode=("a" if append else "w"), title="")

        if "data" in self._file.root:
            self._table = self._file.root.data
            self._check_overlap(first, last, overwrite)
        else:
            self._table = self._file.createTable(self._file.root, 'data', OhlcDescription, "data", filters=filters)
            self._table.cols.time.create_csindex()
            self._table.autoindex = False

    
    def add (self, t, open, high, low, close, volume, open_interest, actual=None, raw=False):
        """
        Add a value to the time series.
        """
        row = self._table.row
        if raw:
            row['time'] = t
        else:
            row['time'] = common.Time.tick(t)
        row['open'] = open
        row['high'] = high
        row['low'] = low
        row['close'] = close
        row['volume'] = volume
        row['openInterest'] = open_interest
        row['actual'] = actual
        row.append()


class Ohlc (object): #, metaclass = TracedMethods(common.log, "__init__", "add", "close")):

    class Ohlc_table (IsDescription):
        time            = Int64Col()
        open            = Float64Col()
        high            = Float64Col()
        low             = Float64Col()
        close           = Float64Col()
        volume          = Float64Col()
        openInterest    = Float64Col()
        actual          = Float64Col()

    def __init__ (self, series, filters):
        self._series = series
        self._local = common.store.write(common.Path.resolve_path("series_ohlc", series)).__enter__()
        self._file = openFile(self._local.local().name, mode="a", title="")

        if "data" in self._file.root:
            self._table = self._file.root.data
                
        else:
            self._table = self._file.createTable(self._file.root, 'data', Ohlc.Ohlc_table, "data", filters=filters)
            self._table.cols.time.create_csindex()
            self._table.autoindex = False

    def read (self, start, stop):
        """
        Read data time ordered.
        """
        return self._table.read_sorted(self._table.cols.time, start=start, stop=stop)

    def read_iter (self, start, stop):
        """
        Read data time ordered using an iterator.
        """
        return self._table.itersorted(self._table.cols.time, start=start, stop=stop)

    def add (self, t, open, high, low, close, volume, open_interest, actual=None, raw=False):
        """
        Add a value to the time series.
        """
        row = self._table.row
        if raw:
            row['time'] = t
        else:
            row['time'] = common.Time.tick(t)
        row['open'] = open
        row['high'] = high
        row['low'] = low
        row['close'] = close
        row['volume'] = volume
        row['openInterest'] = open_interest
        row['actual'] = actual
        row.append()

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



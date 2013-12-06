"""
Data class - open, high, low, close, volume and open interest.

"""

from autologging import logged, traced, TracedMethods
import time
from lib import common
from lib import schema
from tables import *


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
        self._file = openFile(self._local.local().name, mode="w", title="")

        if "data" in self._file.root:
            self._table = self._file.root.data
        else:
            self._table = self._file.createTable(self._file.root, 'data', Ohlc.Ohlc_table, "data", filters=filters)

    def add (self, t, open, high, low, close, volume, open_interest, actual=None, raw=False):
        """
        Add a value to the time series.
        """
        row = self._table.row
        if raw:
            row['time'] = t
        else:
            row['time'] = (time.mktime(t.timetuple())*1000000) + int("%s" % t.microsecond)
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
        # update cache info
        count = int(self._table.nrows)
        self._series.count = count
        self._series.start = -1 if count == 0 else int(self._table[0]['time']) 
        self._series.end = -1 if count == 0 else int(self._table[-1]['time'])
        # close and update 
        self._file.close()
        self._local.save()
        self._local.__exit__(None, None, None)
        # write to db
        schema.save(self._series)



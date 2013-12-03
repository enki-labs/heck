"""
Data class - open, high, low, close, volume and open interest.

"""

from autologging import logged, traced, TracedMethods
import time
from lib import common
from tables import *


@traced(common.log)
def get_store (series_info):
    """
    Open a store.
    """
    filters = Filters(complevel = 9, complib = "blosc", fletcher32 = False)
    return Ohlc(common.Path.get("store", series_info), filters)


class Ohlc (object, metaclass = TracedMethods(common.log, "__init__", "add", "close")):

    class Ohlc_table (IsDescription):
        time            = Int64Col()
        open            = Float64Col()
        high            = Float64Col()
        low             = Float64Col()
        close           = Float64Col()
        volume          = Float64Col()
        openInterest    = Float64Col()
        actual          = Float64Col()


    def __init__ (self, file_path, filters):

        self._file = openFile(file_path, mode="w", title="")

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
            row['time'] = (time.mktime(t.timetuple())*1000000) + long("%s" % t.microsecond)
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
        self._file.close()



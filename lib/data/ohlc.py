"""
Data class - open, high, low, close, volume and open interest.

"""

import time
import config
from tables import *

class Ohlc (object):


    class Ohlc_table (IsDescription):
        time            = Int64Col()
        open            = Float64Col()
        high            = Float64Col()
        low             = Float64Col()
        close           = Float64Col()
        volume          = Float64Col()
        openInterest    = Float64Col()


    def __init__ (self, hdf_file, filters):
        if "data" in hdf_file.root:
            self._table = hdf_file.root.data
        else:
            self._table = hdf_file.createTable(hdf_file.root, 'data', Ohlc.Ohlc_table, "data", filters=filters)


    @staticmethod
    def store (ident):
        """
        Open a store.
        """
        filters = Filters(complevel = 9, complib = "blosc", fletcher32 = False)
        return Ohlc(config.Path.get("store", ident), filters)


    def add (self, t, open, high, low, close, volume, open_interest, raw=False):
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
        row.append()


    def close (self):
        """
        Close the file and flush data.
        """
        self._table.flush()



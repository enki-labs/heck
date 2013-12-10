"""
Data class - open, high, low, close, volume and open interest.

"""

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

        try:
            self._file = open_file(self._local.local().name, mode=("a" if append else "w"), title="")

            try:
                if "data" in self._file.root:
                    self._table = self._file.root.data
                    self._check_overlap(first, last, overwrite)
                else:
                    common.log.info("creating file")
                    self._table = self._file.createTable(self._file.root, 'data', OhlcDescription, "data", filters=filters)
                    self._table.cols.time.create_csindex()
                    self._table.autoindex = False
            except Exception:
                self._file.close()
                self._local.__exit__(None, None, None)
                raise
        except Exception:
            self._local.__exit__(None, None, None)
            raise
            

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


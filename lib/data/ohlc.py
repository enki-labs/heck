"""
Data class - open, high, low, close, volume and open interest.

"""

import time
from lib import common
from lib import schema
from lib.data import update_series, Writer, Reader, InnerWriter, WriterLock
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


def adjuster (row, adjustment):
    return {"time": row["time"],
            "open": row["open"] + adjustment,
            "high": row["high"] + adjustment,
            "low": row["low"] + adjustment,
            "close": row["close"] + adjustment,
            "volume": row["volume"],
            "openInterest": row["openInterest"],
            "actual": row["actual"]}


class OhlcReader (Reader):
    """
    Read an OHLC data store.
    """

    def __init__ (self, series):
        super(OhlcReader, self).__init__(series, "series_ohlc")


class OhlcInnerWriter (InnerWriter):

    def __init__ (self, writer_lock, first, last):
        super().__init__(writer_lock, first, last)

    def add (self, t, open, high, low, close, volume, open_interest, actual=None, raw=False):
        """
        Add a value to the time series.
        """
        row = self._writer_lock._table.row
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
    

class OhlcWriterLock (WriterLock):
    """
    A writable OHLC data store with early locking.
    """

    def __init__ (self, series, filters, overwrite, append=True):
        super().__init__(series, filters, overwrite, append, "series_ohlc", OhlcDescription)

    def writer (self, first, last):
        return OhlcInnerWriter(self, first, last)


class OhlcWriter (Writer):
    """
    A writable OHLC data store.
    """
    
    def __init__ (self, series, filters, first, last, overwrite, append=True):
        super().__init__(series, filters, first, last, overwrite, append, "series_ohlc", OhlcDescription)

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


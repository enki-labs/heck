"""
Data class - open, high, low, close, volume and open interest.

"""

import time
from lib import common
from lib import schema
from lib.data import update_series, Writer, Reader
from tables import *


class TickDescription (IsDescription):
    """
    Tick data table description.
    """
    time            = Int64Col()
    event           = Int32Col()
    price           = Float64Col()
    volume          = Float64Col()
    qualifier       = StringCol(1000)
    acc_volume      = Float64Col()


class TickEventEnum (object):
    """
    Defines the event enum mapping to integer value.
    """

    def __init__ (self):
        self._forward = dict(trade=0, settle=100, openinterest=200)
        #self._reverse = dict(0="trade", 100="settle", 200="openinterest")        
        self._reverse = dict()
        self._reverse[0] = "trade"
        self._reverse[100] = "settle"
        self._reverse[200] = "openinterest"
 
    def lookup (self, name=None, value=None):
        """ Encode/decode """
        if name: return self._forward[name]
        if value: return self._reverse[value]


class TickReader (Reader):
    """
    Read a Tick data store.
    """

    def __init__ (self, series):
        super().__init__(series, "series_tick")
        self.event_enum = TickEventEnum()


class TickWriter (Writer):
    """
    A writable Tick data store.
    """
    
    def __init__ (self, series, filters, first, last, overwrite, append=True):
        super().__init__(series, filters, first, last, overwrite, append, "series_tick", TickDescription)
        self.event_enum = TickEventEnum()

    def add_row (self, row):
        """
        Add a raw reader row to the time series.
        """
        #TODO more efficient raw append
        new_row = self._table.row
        new_row["time"] = row["time"]
        new_row["event"] = row["event"]
        new_row["price"] = row["price"]
        new_row["volume"] = row["volume"]
        new_row["qualifier"] = row["qualifier"]
        new_row["acc_volume"] = row["acc_volume"]
        new_row.append() 

    def add (self, t, event, price, volume, qualifier, acc_volume, raw=False):
        """
        Add a value to the time series.
        """
        row = self._table.row
        if raw:
            row['time'] = t
        else:
            row['time'] = common.Time.tick(t)
        row['event'] = event
        row['price'] = price
        row['volume'] = volume
        row['qualifier'] = qualifier
        row['acc_volume'] = acc_volume
        row.append()

    def save (self):
        """
        Write new file to store.
        """
        if self._store:
            self._store.save()


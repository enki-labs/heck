"""
Instrument class accessing metadata, mappings etc.

"""

from autologging import logged, traced, TracedMethods
from lib import common
import schema
from sqlalchemy.sql import and_


@traced(common.log)
def get_reuters (symbol):
    """ Get an instrument using a Reuters symbol """
    symbol_resolve = schema.table("symbol_resolve")
    args = and_(symbol_resolve.columns.symbol==symbol, symbol_resolve.columns.source=="reuters")
    matches = schema.select(symbol_resolve, args)
    if matches.rowcount == 0:
        raise Exception("Cannot map Reuters instrument %s" % symbol)
    return Instrument(matches.fetchone()[2])


@traced(common.log)
def get_bloomberg (symbol):
    """ Get and instrument using a Bloomberg symbol """
    return Instrument(symbol)


class Instrument (object, metaclass = TracedMethods(common.log, "__init__")):
    """ Define an instrument and its metadata """

    def __init__ (self, symbol, load=True):
        self._symbol = symbol
        if load:
            self._data = self._load()

    def _load (self):
        instrument = schema.table("instrument")
        args = and_(instrument.columns.symbol==self._symbol)
        matches = schema.select(instrument, args)
        if matches.rowcount == 0:
            raise Exception("Unknown instrument %s" % self._symbol)
        return matches.fetchone()

    

"""
Instrument class accessing metadata, mappings etc.

"""

from autologging import logged, traced, TracedMethods
import json
from lib import common
from lib import schema
from sqlalchemy.sql import and_


@traced(common.log)
def get_reuters (symbol):
    """ Get an instrument using a Reuters symbol """
    symbol_resolve = schema.table("symbol_resolve")
    args = and_(symbol_resolve.columns.symbol==symbol, symbol_resolve.columns.source=="reuters")
    matches = schema.select(symbol_resolve, args)
    if matches.rowcount == 0:
        raise Exception("Cannot map Reuters instrument %s" % symbol)
    return Series(matches.fetchone()[2])


@traced(common.log)
def get_bloomberg (symbol):
    """ Get and instrument using a Bloomberg symbol """
    try:
        return Series(symbol)
    except:
        return None


class Series (object, metaclass = TracedMethods(common.log, "__init__")):
    """ Define an instrument and its metadata """

    def __init__ (self, symbol, load=True):
        """ Ctor - load data from DB on request """
        self._symbol = symbol
        self._metadata = dict()
        if load:
            self._load()

    def _load (self):
        """ Load the symbol from the database """
        data = schema.select_one("series", symbol=self._symbol)
        if data:
            self._metadata = json.loads(data.meta)
        else:
            raise Exception("No series %s" % self._symbol)

    def set (self, name, value):
        """ Series meta data """
        self._metadata[name] = value

    def save (self):
        """ Persist to the database """
        inst = schema.table("series")
        inst.symbol = self._symbol
        inst.meta = json.dumps(self._metadata)
        schema.save(inst)


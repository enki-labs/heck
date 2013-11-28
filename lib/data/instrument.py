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
    print(dir(matches))
    if matches.rowcount == 0:
        raise Exception("Unknown instrument %s" % symbol)
    return matches.fetchone()[2]

class Instrument (object, metaclass = TracedMethods(common.log, "__init__")):
    """ Define an instrument and its metadata """

    def __init__ (self):
        pass





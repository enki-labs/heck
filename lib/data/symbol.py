"""
Instrument class accessing metadata, mappings etc.

"""

from lib import common
from lib import schema

def get_reuters (symbol):
    """ Get an instrument using a Reuters symbol """
    symbol_resolve = schema.table.symbol_resolve
    args = and_(symbol_resolve.columns.symbol==symbol, symbol_resolve.columns.source=="reuters")
    matches = schema.select(symbol_resolve, args)
    if matches.rowcount == 0:
        raise Exception("Cannot map Reuters instrument %s" % symbol)
    return Series(matches.fetchone()[2])

def get (symbol, create):
    """ Get symbol definition """
    symbol_instance = schema.select_one("symbol", schema.table.symbol.symbol==symbol)
    if symbol_instance:
        return symbol_instance
    elif create:
        symbol_instance = schema.table.symbol()
        symbol_instance.symbol = symbol
        return symbol_instance
    else:
        raise Exception("Unknown symbol (%s)", symbol)


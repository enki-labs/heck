"""
Import a Bloomberg data file.

"""

import pytz
import calendar
from datetime import datetime
import re
from lib.data import symbol
from lib import common
from lib import schema


"""
Search for a valid contract.
"""
contract_search = re.compile("^.*[F,G,H,J,K,M,N,Q,U,V,X,Z]\d$")

"""
Search for a year code.
"""
year_search = re.compile("\d$")

"""
Search for a month code.
"""
month_search = re.compile("[F,G,H,J,K,M,N,Q,U,V,X,Z](?=\d$)")

"""
Future month codes.
"""
month_codes = dict(F=1, G=2, H=3, J=4, K=5, M=6, N=7, Q=8, U=9, V=10, X=11, Z=12)


def parse_future_contract (symbol, reference_date):
    """
    Parse a future contract month/year from a symbol.
    """
    if contract_search.match(symbol):
        year_code = year_search.findall(symbol)[0]
        month_code = month_search.findall(symbol)[0]
        symbol = symbol[:len(symbol)-(len(year_code)+len(month_code))]
        year = int(year_code)
        month = month_codes[month_code]

        # find valid contract year given reference date
        for check_year in range(1900 + year, 2200, 10):
            check_date = datetime(check_year, month, calendar.monthrange(check_year, month)[1]).replace(tzinfo=pytz.utc)
            if check_date < reference_date: continue
            else:
                year = check_year
                break

        return dict(symbol=symbol, year=str(year), month=str(month))
    else:
        return None

def parse (symbol, reference_date):
    """
    Parse symbol to determine attributes.
    """
    definition = dict(provider="reuters")
    contract = parse_future_contract(symbol, reference_date)
    if contract:
        definition["symbol"] = contract["symbol"]
        definition["year"] = contract["year"]
        definition["month"] = contract["month"]
    else:
        definition["symbol"] = symbol    

    #map symbol to Bloomberg
    symbol_map = schema.select_one("symbol_resolve"
                                 , schema.table.symbol_resolve.symbol==definition["symbol"]
                                 , schema.table.symbol_resolve.source=="reuters" )
    if symbol_map:
        definition["symbol"] = symbol_map.resolve
    else:
        raise Exception("Cannot resolve Reuter symbol (%s)" % (definition["symbol"]))
    
    return definition


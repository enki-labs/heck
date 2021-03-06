"""
Import a Bloomberg data file.

"""

import pytz
import re
from lib.data import symbol
from lib import common
from lib import schema


"""
Search for a valid contract.
"""
contract_search = re.compile("^.*[F,G,H,J,K,M,N,Q,U,V,X,Z]\d{1,2}$")

"""
Search for a contract name (eg Mar08).
"""
contract_name_search = re.compile("^.*\w[Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec][0-9][0-9]$")

"""
Search for a year code.
"""
year_search = re.compile("\d{1,2}$")

"""
Search for a month code.
"""
month_search = re.compile("[F,G,H,J,K,M,N,Q,U,V,X,Z](?=\d{1,2}$)")

"""
Future month codes.
"""
month_codes = dict(F=1, G=2, H=3, J=4, K=5, M=6, N=7, Q=8, U=9, V=10, X=11, Z=12)


def parse_future_contract (symbol):
    """
    Parse a future contract month/year from a symbol.
    """
    if contract_search.match(symbol):
        year_code = year_search.findall(symbol)[0]
        month_code = month_search.findall(symbol)[0]
        symbol = symbol[:len(symbol)-(len(year_code)+len(month_code))]
        year = int(year_code)
        month = month_codes[month_code]

        if year < 10 and len(year_code) == 1:
            year = 2010 + year
        elif year < 50:
            year = 2000 + year
        else:
            year = 1900 + year

        return dict(symbol=symbol, year=str(year), month=str(month))
    else:
        return None

def parse_symbol (symbol):
    """
    Parse symbol to determine attributes.
    """
    definition = dict(provider="bloomberg")
    parts = symbol.split(" ")

    if len(parts[0]) == 1 or len(parts) == 4:
        tempsymbol = parts[0] + " " + parts[1]
        parts = parts[1:]
        parts[0] = tempsymbol

    partcount = len(parts)

    if partcount in (2,3):

        if partcount == 2:
            definition["class"] = parts[1].lower()

        elif len(parts) == 3:
            definition["source"] = parts[1]
            definition["class"] = parts[2].lower()

        contract = parse_future_contract(parts[0])
        if contract:            
            definition["symbol"] = contract["symbol"]
            definition["month"] = contract["month"]
            definition["year"] = contract["year"]
        else:
            definition["symbol"] = parts[0]
    else:
        raise Exception("Cannot parse symbol %s" % symbol)

    return definition


class Import (object):

    @staticmethod
    def handler_future (vals, inst):
        """
        Parse Future details
        """
        return inst

    @staticmethod
    def handler_index (vals):
        """
        Parse Index details
        """
        return inst

    @staticmethod
    def handler_swap (vals):
        """
        Parse Swap details
        """
        pass

    @staticmethod
    def handler_fx (vals):
        """
        Parse FX details
        """
        pass

    @staticmethod
    def handler_equity (vals):
        """
        Parse Equity details
        """
        pass

    @staticmethod
    def handler_forward (vals):
        """
        Parse Forward details
        """
        pass

    @staticmethod
    def parse (reader):
        """
        Parse a data file.
        """
        reading = False
        line_count = 0

        handlers = dict()
        handlers["Future"] = lambda x: parse_future(x)
        handlers["Index"] = lambda x: parse_index(x)
        handlers["SWAP SPREAD"] = lambda x: parse_swap(x)
        handlers["FWD SWAP"] = lambda x: parse_swap(x)
        handlers["CROSS"] = lambda x: parse_fx(x)
        handlers["SWAP"] = lambda x: parse_swap(x)
        handlers["NON-DELIVERABLE IRS SWAP"] = lambda x: parse_swap(x)
        handlers["SWAPTION VOLATILITY"] = lambda x: parse_swap(x)
        handlers["Common Stock"] = lambda x: parse_equity(x)
        handlers["SPOT"] = lambda x: parse_fx(x)
        handlers["NDF SWAP"] = lambda x: parse_swap(x)
        handlers["ONSHORE SWAP"] = lambda x: parse_swap(x)

        for line in reader.readlines():
            line_count = line_count + 1
            if line_count % 500 == 0: common.log.debug("line %s" % line_count)
            if not isinstance(line, str): 
                line = line.decode("utf-8").strip()
            else:
                line = line.strip()
            if not reading and line == "START-OF-DATA":
                common.log.debug("start reading")
                reading = True
            elif reading and line == "END-OF-DATA":
                common.log.debug("stop reading")
                break
            elif reading:
                vals = line.split("|")
                if int(vals[1]) == 0:
                    tags = parse_symbol(vals[0])
                    sym = symbol.get(tags["symbol"], create=True)
                    category = vals[23].lower()
                    sym.category = category
                    if category == "future" and contract_name_search.match(vals[4]):
                        sym.name = " ".join(vals[4][:-5].split()) #collapse whitespace
                    else:
                        sym.name = " ".join(vals[4].split())
                    if len(vals[8].strip()) > 0: sym.meta_add("currency", vals[8])
                    if len(vals[14].strip()) > 0: sym.meta_add("exchange", vals[14])
                    sym.meta_add("class", tags["class"])
                    schema.save(sym)
                    #raise Exception("Unknown type %s" % vals[23])

        common.log.debug("parsed %s lines" % line_count)



"""
Import a Bloomberg data file.

"""

import datetime
import pytz
from lib import data
from lib import common
from lib import schema
from lib.inbound import reuters_symbol
from lib.data import ohlc


class PriceAdjustment (object):

    def __init__ (self, symbol):
        symboldef = schema.select_one("symbol_resolve", schema.table.symbol_resolve.resolve==symbol
                                          , schema.table.symbol_resolve.source=="reuters")
        if not symboldef: raise Exception("Cannot resolve %s" % (symbol))
        self._adjustments = symboldef.adjust_dict()["price_adjustments"]
        self._index = 0
        self._count = len(self._adjustments)

    def next (self):
        multiplier = 1.0
        next_time = None

        if self._index < self._count:
            multiplier = self._adjustments[self._index]["multiplier"]
            self._index += 1
            if self._index < self._count:
                next_time = self._adjustments[self._index]["start"]

        return (multiplier, next_time)


class Import (object):

    @staticmethod
    def parse_value (val):
        """
        Parse a text value to float.
        """
        val = val.strip()
        if len(val) == 0:
            return None
        else:
            return float(val)

    @staticmethod
    def parse_time (datepart, timepart):
        """
        Parse a Reuters time string.
        """
        return common.Time.tick_parts( int(datepart[0:4])
                                     , int(datepart[4:6])
                                     , int(datepart[6:8])
                                     , int(timepart[0:2])
                                     , int(timepart[3:5])
                                     , int(timepart[6:8])
                                     , int(timepart[9:]) )

    @staticmethod
    def parse (reader, progress):
        """
        Parse a data file.
        """
        #read second line
        reader.readline()
        second_line = reader.readline().decode("utf-8").strip()
        progress.progress_end_increment() #header line not included in count
        
        #read line count
        line = None
        for line in reader.readlines():
            progress.progress_end_increment()
        if not line:
            raise Exception("Inbound file is empty")
        last_line = line.decode("utf-8").strip()
        reader.seek(0, 0)
        reader.readline() #skip header

        tags = dict(format="ohlc")
        second_line_parts = second_line.split(",")

        if second_line_parts[5] == "Intraday 1Sec":
            tags["period"] = "1sec"
        else:
            raise Exception("Unknown period %s" % second_line_parts[5])

        ticker = second_line_parts[0]
        first = Import.parse_time(second_line_parts[2], second_line_parts[3])
        tags.update(reuters_symbol.parse(ticker, common.Time.time(first)))

        last_line_parts = last_line.split(",")
        last = Import.parse_time(last_line_parts[2], last_line_parts[3])
        price_adjustment = PriceAdjustment(tags["symbol"])
        
        with data.get_writer(tags, first=first, last=last, create=True, append=True) as writer:
            print(common.Time.time(first))
            multiplier, next_adjustment = price_adjustment.next()
            for line in reader.readlines():
                line = line.decode("utf-8").strip()
                progress.progress()

                parts = line.split(",")
                time = Import.parse_time(parts[2], parts[3])

                if next_adjustment != None and time >= next_adjustment:
                    multiplier, next_adjustment = price_adjustment.next()

                priceopen = Import.parse_value(parts[6])
                pricehigh = Import.parse_value(parts[7])
                pricelow = Import.parse_value(parts[8])
                priceclose = Import.parse_value(parts[9])
                volume = Import.parse_value(parts[10])
                writer.add( time
                          , (priceopen if priceopen == None else priceopen * multiplier)
                          , (pricehigh if pricehigh == None else pricehigh * multiplier)
                          , (pricelow if pricelow == None else pricelow * multiplier)
                          , (priceclose if priceclose == None else priceclose * multiplier)
                          , volume
                          , 0
                          , raw=True )
            writer.save()



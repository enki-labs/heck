"""
Import a Bloomberg data file.

"""

import datetime
import pytz
from lib import data
from lib import common
from lib import schema
from lib.inbound import reuters_symbol
from lib.data import tick


_tick_event = tick.TickEventEnum()

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
                next_time = adjustments[self._index]["start"]

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
    def parse_event (name):
        """
        Parse event name, map to enum.
        """
        if name == "Trade": return _tick_event.lookup(name="trade")
        if name == "Settlement Price": return _tick_event.lookup(name="settle")
        if name == "Open Interest": return _tick_event.lookup(name="openinterest")
        return None

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
        for line in reader.readlines():
            progress.progress_end_increment()
        last_line = line.decode("utf-8").strip()
        reader.seek(0, 0)
        reader.readline() #skip header

        ##read last line
        #reader.seek(-2, 2)
        #while reader.read(1) != b"\n":
        #    reader.seek(-2, 1) # seek backwards
        #last_line = reader.readline().decode("utf-8").strip()
        #reader.seek(0, 0)
        
        tags = dict(format="tick", period="tick")
        second_line_parts = second_line.split(",")
        ticker = second_line_parts[0]
        first = Import.parse_time(second_line_parts[2], second_line_parts[3])
        tags.update(reuters_symbol.parse(ticker, common.Time.time(first)))
        last_line_parts = last_line.split(",")
        last = Import.parse_time(last_line_parts[2], last_line_parts[3])
        price_adjustment = PriceAdjustment(tags["symbol"])
        
        with data.get_writer(tags, first=first, last=last, create=True, append=False) as writer:

            multiplier, next_adjustment = price_adjustment.next()

            for line in reader.readlines():
                line = line.decode("utf-8").strip()
                progress.progress()

                parts = line.split(",")
                event = Import.parse_event(parts[5])
                if event != None:
                    time = Import.parse_time(parts[2], parts[3])

                    if next_adjustment != None and time >= next_adjustment:
                        multiplier, next_adjustment = price_adjustment.next()

                    price = Import.parse_value(parts[6])
                    writer.add( time
                              , event
                              , (price if price == None else price * multiplier)
                              , Import.parse_value(parts[7])
                              , parts[8]
                              , Import.parse_value(parts[11])
                              , raw=True )
            writer.save()



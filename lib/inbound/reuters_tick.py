"""
Import a Bloomberg data file.

"""

import datetime
import pytz
from lib import data
from lib import common
from lib.inbound import reuters_symbol
from lib.data import tick


_tick_event = tick.TickEventEnum()

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
    def parse (reader):
        """
        Parse a data file.
        """
        line_count = 0

        #read second line
        reader.readline()
        second_line = reader.readline().decode("utf-8").strip()
        
        #read last line
        reader.seek(-2, 2)
        while reader.read(1) != b"\n":
            reader.seek(-2, 1) # seek backwards
        last_line = reader.readline().decode("utf-8").strip()
        reader.seek(0, 0)
        
        tags = dict(format="tick", period="tick")
        second_line_parts = second_line.split(",")
        ticker = second_line_parts[0]
        first = Import.parse_time(second_line_parts[2], second_line_parts[3])
        tags.update(reuters_symbol.parse(ticker, common.Time.time(first)))
        last_line_parts = last_line.split(",")
        last = Import.parse_time(last_line_parts[2], last_line_parts[3])
 
        with data.get_writer(tags, first=first, last=last, create=True, append=False) as writer:
            for line in reader.readlines():
                line = line.decode("utf-8").strip()
                line_count = line_count + 1
                if line_count == 1: continue #skip header

                if line_count % 100000 == 0: common.log.debug("line %s" % line_count)

                parts = line.split(",")
                event = Import.parse_event(parts[5])
                if event != None:
                    writer.add( Import.parse_time(parts[2], parts[3])
                              , event
                              , Import.parse_value(parts[6])
                              , Import.parse_value(parts[7])
                              , parts[8]
                              , Import.parse_value(parts[11])
                              , raw=True )
            writer.save()
        common.log.debug("parsed %s lines" % line_count)



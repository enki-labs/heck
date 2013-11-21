"""
Import a Bloomberg data file.

"""

from autologging import logged, traced, TracedMethods
import pytz
from lib.data import ohlc
from lib import common


class Import (object, metaclass= TracedMethods(common.log, "parse", "parse_value")):

    #@staticmethod
    #def parse_file (filename):
    #    """
    #    Parse a data file.
    #    """
    #    with open(filename, 'r') as content:
    #        Import.parse(content)


    @staticmethod
    def parse_value (val):
        """
        Parse a text value to float.
        """
        val = val.strip()
        if len(val) == 0 or val == "N.A.":
            return None
        else:
            return float(val)


    @staticmethod
    def parse (reader):
        """
        Parse a data file.
        """
        reading = False
        store = None
        ticker = ""
        line_count = 0

        for line in reader.readlines():
            line_count = line_count + 1
            common.log.debug(line_count)
            line = line.decode("utf-8").strip()
            if not reading and line == "START-OF-DATA":
                common.log.debug("start reading")
                reading = True
            elif reading and line == "END-OF-DATA":
                common.log.debug("stop reading")
                break
            elif reading:
                vals = line.split("|")

                if ticker == "":
                    common.log.debug("first ticker")
                    ticker = vals[0]
                    store = ohlc.get_store(ticker)
                elif ticker != vals[0]:
                    ticker = vals[0]
                    store.close()
                    store = ohlc.get_store(ticker)

                if len(vals[3].strip()) == 0: 
                    tickTime = datetime.datetime.strptime(vals[3], "%Y/%m/%d")
                    tickTime = tickTime.replace(tzinfo=pytz.utc)
                    store.add(tickTime, Import.parse_value(vals[4])
                                  , Import.parse_value(vals[5])
                                  , Import.parse_value(vals[6])
                                  , Import.parse_value(vals[7])
                                  , Import.parse_value(vals[8])
                                  , Import.parse_value(vals[9])
                                  , Import.parse_value(vals[10])) 

        if store:
            store.close()

        common.log.debug("parsed %s lines" % line_count)



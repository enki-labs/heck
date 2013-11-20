"""
Import a Bloomberg data file.

"""

import pytz
import ohlc from data

class Import (object):

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

        for line in reader.readlines():
            line = line.strip()
            if not reading and line == "START-OF-DATA":
                reading = True
            elif reading and line == "END-OF-DATA":
                break
            elif reading:
                vals = line.split("|")

                if ticker == "":
                    ticker = vals[0]
                    store = Ohlc.store(ticker)
                elif ticker != vals[0]:
                    ticker = vals[0]
                    store.close()
                    store = Ohlc.store(ticker)
 
                tickTime = datetime.datetime.strptime(vals[3], "%Y/%m/%d")
                tickTime = tickTime.replace(tzinfo=pytz.utc)
                store.add(tickTime, Import.parse_value(vals[4])
                                  , Import.parse_value(vals[5])
                                  , Import.parse_value(vals[6])
                                  , Import.parse_value(vals[7])
                                  , Import.parse_value(vals[8])
                                  , Import.parse_value(vals[9])) 

        if store:
            store.close()


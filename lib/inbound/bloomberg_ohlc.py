"""
Import a Bloomberg data file.

"""

import datetime
import pytz
from lib import data
from lib import common
from lib.inbound import bloomberg_symbol


class Import (object):

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
        cache = []

        for line in reader.readlines():
            line_count = line_count + 1
            if line_count % 1000 == 0: common.log.debug("line %s" % line_count)
            line = line.decode("utf-8").strip()
            if not reading and line == "START-OF-DATA":
                common.log.debug("start reading")
                reading = True
            elif reading and line == "END-OF-DATA":
                common.log.debug("stop reading")
                break
            elif reading:
                vals = line.split("|")
                has_date = len(vals[3].strip()) != 0

                if has_date:
                    if ticker == "":
                        common.log.debug("first ticker")
                        ticker = vals[0]
                    elif ticker != vals[0]:
                        if len(cache) > 0:
                            tags = dict(format="ohlc", period="1day")
                            tags.update(bloomberg_symbol.parse_symbol(ticker))
                            common.log.info("%s" % (tags))
                            try:
                                store = data.get_writer(tags, first=cache[0][0], last=cache[-1][0], create=True, append=True)
                                for row in cache:
                                    store.add(*row)
                                store.close()
                            except data.OverlapException:
                                common.log.info("ignore overlapping data")
                            cache = []

                        ticker = vals[0]

                    if len(vals[3].strip()) != 0: 
                        tickTime = datetime.datetime.strptime(vals[3], "%Y/%m/%d")
                        tickTime = tickTime.replace(tzinfo=pytz.utc)
                        cache.append([ tickTime
                                     , Import.parse_value(vals[4])
                                     , Import.parse_value(vals[5])
                                     , Import.parse_value(vals[6])
                                     , Import.parse_value(vals[7])
                                     , Import.parse_value(vals[8])
                                     , Import.parse_value(vals[9])
                                     , Import.parse_value(vals[10]) ])

        common.log.debug("parsed %s lines" % line_count)



"""
Import a Bloomberg data file.

"""

import datetime
import pytz
from lib import data
from lib import common
from lib.inbound import bloomberg_symbol
import exception

import requests
import json

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
    def parse (reader, progress):
        """
        Parse a data file.
        """
        reading = False
        store = None
        ticker = ""
        cache = []
        cache_data = []
        
        
        for line in reader.readlines():
            progress.progress_end_increment()
        reader.seek(0)

        for line in reader.readlines():
            progress.progress()

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
                        ticker = vals[0]
                    elif ticker != vals[0]:
                        if len(cache_data) > 0:
                            tags = dict(format="ohlc", period="1day")
                            tags.update(bloomberg_symbol.parse_symbol(ticker))
                            cache.append({"tags": tags, "rows": cache_data})
                            #params = {"action": "add", "tags": json.dumps(tags), "data": json.dumps(cache)}
                            #req = requests.post("http://beta.taustack.com:3030/internal/store", data=params)
                            #try:
                            #    with data.get_writer(tags, first=cache[0][0], last=cache[-1][0], create=True, append=True) as writer:
                            #        for row in cache:
                            #            writer.add(*row)
                            #        writer.save()
                            #except exception.OverlapException:
                            #    common.log.info("ignore overlapping data")
                        ticker = vals[0]
                        cache_data = []

                    if len(vals[3].strip()) != 0: 
                        tickTime = datetime.datetime.strptime(vals[3], "%Y/%m/%d")
                        tickTime = tickTime.replace(tzinfo=pytz.utc)
                        cache_data.append([ common.Time.tick(tickTime)
                                     , Import.parse_value(vals[4])
                                     , Import.parse_value(vals[5])
                                     , Import.parse_value(vals[6])
                                     , Import.parse_value(vals[7])
                                     , Import.parse_value(vals[8])
                                     , Import.parse_value(vals[9])
                                     , Import.parse_value(vals[10]) ])

        params = {"action": "add", "data": json.dumps(cache)}
        req = requests.post("http://beta.taustack.com:3030/internal/store", data=params)


"""
Data class - open, high, low, close, volume and open interest.

"""

import time
import pytz
import json
import dateutil
from lib import common
from lib import schema
from lib.data import decode_tags, resolve_tags, update_series, Writer, Reader, InnerWriter, WriterLock


class ComputeReader (object):

    def __init__ (self, series, tags):
        self._series = series
        self._tags = tags
        self._series_list = []
        self._reader = None
        self._count = 0
        self._adjuster = None
        self._adjuster_columns = []

    def columns (self):
        return self._adjuster_columns

    def __enter__ (self):

        from lib.data import ohlc
        from lib.data import tick
        if "format" not in self._tags:
            raise Exception("Missing format tag")
        elif self._tags["format"] == "ohlc":
            self._reader = lambda series : ohlc.OhlcReader(series) 
            self._adjuster = ohlc.adjuster
            self._adjuster_columns = ohlc.adjuster_columns()
        elif self._tags["format"] == "tick":
            self._reader = lambda series : tick.TickReader(series)
            self._adjuster = tick.adjuster
            self._adjuster_columns = tick.adjuster_columns()
        else:
            raise Exception("Unknown series format %s" % (self._tags["format"]))

        if "compute" in self._series.meta_dict:
            self._series_list = self._series.meta_dict["compute"]
            self._count = self._series.count
        else:
            self._load_state()
            self._series.meta_dict["compute"] = self._series_list
            self._series.count = self._count
            schema.save(self._series)
        return self
    
    def _load_state (self):
        config_tags = {"compute": self._tags["compute"], "symbol": self._tags["symbol"]}
        config_tags_ids = resolve_tags(config_tags, create=False)
        self._config = schema.select_one("config", schema.table.config.tags==config_tags_ids)
        if not self._config:
            raise Exception("No config for computed series %s" % (config_tags))

        self._tags.pop("compute", None)
        start_tick = None
        virtual_index = 0

        for series in json.loads(self._config.content)["series"]:
            search_tags = dict(list(self._tags.items()) + list(series["tags"].items()))
            search_tags_ids = resolve_tags(search_tags, create=False)
            series_entity = schema.select_one("series", schema.table.series.tags==search_tags_ids)
            if start_tick == None and series_entity == None:
                continue #skip to first series
            elif series_entity == None:
                raise Exception("Missing series %s" % (search_tags))
            first_index = 0
            with self._reader(series_entity) as reader:
                if start_tick == None: first_index = 0
                else:
                    first_index = reader.nearest(end_tick)
                    for row in reader.read(start=first_index, stop=first_index):
                        start_tick = row["time"]
                        break
                end_time = dateutil.parser.parse(series["end"]).replace(tzinfo=pytz.timezone(series["timezone"]))
                end_tick = common.Time.tick(end_time.astimezone(pytz.utc))
                last_index = reader.nearest(end_tick)
                count = (last_index - first_index)
                self._series_list.append({"series": series_entity.id, "adjust": series["adjust"], "end": end_tick, "cumulative_adjust": 0, "virtual_index": virtual_index, "count": count, "first_index": first_index, "last_index": last_index})
                start_tick = end_tick
                virtual_index += count
                self._count += count

        cumulative_adjust = 0
        for index in range(len(self._series_list)-1, -1, -1):
            cumulative_adjust += self._series_list[index]["adjust"]
            self._series_list[index]["cumulative_adjust"] += cumulative_adjust
        common.log.info("loaded compute state")
  
    def __exit__ (self, typ, value, tb):
        pass

    def read_iter (self, start=None, stop=None, step=1, no_index=False):
        return Wrapper(start, stop, step, self._series_list, self._reader, self._adjuster)

    def read (self, start=None, stop=None, step=1, no_index=False):
        return Wrapper(start, stop, step, self._series_list, self._reader, self._adjuster)

    def count (self):
        return self._count


class Wrapper (object):
    def __init__ (self, start, stop, step, series_list, reader, adjuster):

        self._reader = reader
        self._series_list = series_list
        self._series_list_index = 0
        self._step = step
        self._adjust = 0
        self._adjuster = adjuster
        self._last_tick = 0

        if start == None: self._start = 0
        else: self._start = start

        if stop == None: self._stop = -1
        else: self._stop = stop

        self._virtual_index = self._start
        index_count = 0

        for series in self._series_list:
            index_count += series["count"] - 1
            if self._start < (series["virtual_index"] + series["count"]):
               series_entity = schema.select_one("series", schema.table.series.id==series["series"])
               self._current_reader = self._reader(series_entity).__enter__()
               self._next_index = series["virtual_index"] + series["count"]
               local_start = self._start - series["virtual_index"] + series["first_index"]
               self._current_reader_iter = self._current_reader.read_iter(start=local_start, stop=None, step=self._step)
               self._adjust = series["cumulative_adjust"]
               return
            self._series_list_index += 1

        raise Exception("Could not find underlying series") 

    def __iter__ (self):
        return self

    def __next__ (self):
        if self._virtual_index != self._stop:
            if self._virtual_index > self._next_index:
                self._series_list_index += 1
                if len(self._series_list) == self._series_list_index:
                    raise StopIteration()
                next_series = self._series_list[self._series_list_index]
                self._current_reader.__exit__(None, None, None)
                next_series_entity = schema.select_one("series", schema.table.series.id==next_series["series"])
                self._current_reader = self._reader(next_series_entity)
                self._current_reader.__enter__()
                start_index = self._current_reader.nearest(self._last_tick)+1
                self._current_reader_iter = self._current_reader.read_iter(start=start_index, stop=None, step=self._step)
                self._next_index += next_series["count"]
                self._adjust = next_series["cumulative_adjust"]
            self._virtual_index += self._step
            try:
                next_row = self._current_reader_iter.__next__()
                self._last_tick = next_row["time"]
                return self._adjuster(next_row, self._adjust)
            except StopIteration:
                raise
        else:
            raise StopIteration()



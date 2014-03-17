import numpy as np
from sqlalchemy import and_, not_
import pytz
from lib.process import DataFrameWrapper
from lib.process import ProcessBase
from lib import data
from lib import schema


class Process (ProcessBase):


    def __init__ (self, process_def):
        super().__init__(process_def)


    def parameters (self):
        return dict(period="string")


    def generate (self):
        """ Generate multiple series for each input """
        include_tags_any_ids = data.resolve_tags(self._include["any"], create=False)
        include_tags_all_ids = data.resolve_tags(self._include["all"], create=False)
        exclude_tags_ids = data.resolve_tags(self._exclude, create=False)
        if len(exclude_tags_ids) == 0:
            exclude_tags_ids = [0] #always match
        series = schema.table.series
        outputs = []
        with schema.select("series"
                         , series.tags.overlap(include_tags_any_ids)
                         , series.tags.contains(include_tags_all_ids)
                         , not_(series.tags.overlap(exclude_tags_ids))
                         ) as select:
            for selected in select.all():
                tags = data.decode_tags(selected.tags)
                for name, value in self._remove.items():
                    if name in tags:
                        del[name]
                for name, value in self._add.items():
                    tags[name] = value
                if self._params["mode"] == "symbol":
                    symbol = schema.select_one("symbol", schema.table.symbol.symbol == selected.symbol)
                    if symbol and "market_hours" in symbol.meta_dict():
                        for name, detail in symbol.meta_dict()["market_hours"].items():
                            out_tags = tags.copy()
                            out_tags["hours"] = name
                            outputs.append(dict(input_last_modified=selected.last_modified, output_tags=out_tags, output_depend=[selected.id]))
                    else:
                        raise Exception("Cannot find symbol/config %s %s" % (selected.symbol, selected))
                else:
                    raise Exception("Unknown mode parameter %s" % self._params["mode"])
        return outputs 

    def run (self, queued, progress):
        print(queued)
        series = schema.select_one("series", schema.table.series.id==queued.depend[0])
        filtered_data = False

        #tags = data.decode_tags(series.tags)
        #for name, value in self._add.items():
        #    tags[name] = value
        #for name, value in self._remove.items():
        #    if name in tags:
        #        del[name]
        tags = queued.tags_dict

        if series and series.count > 0:

            timezone = None
            open_time = None
            close_time = None

            if self._params["mode"] == "symbol":
                symbol = schema.select_one("symbol", schema.table.symbol.symbol == series.symbol)
                hours_info = symbol.meta_dict()["market_hours"][tags["hours"]]
                timezone = hours_info["timezone"]
                open_time = hours_info["open"]
                close_time = hours_info["close"]
            else:
                raise Exception("Unknown mode parameter %s" % self._params["mode"])

            with data.get_reader_pandas(series) as reader:
                filtered = reader.df.tz_convert(pytz.timezone(timezone)).between_time(open_time, close_time).dropna().tz_convert(pytz.UTC)

                if len(filtered) > 0:
                    first = filtered.head(1).index[0].value
                    last = filtered.tail(1).index[0].value
                    with data.get_writer(tags, first, last, create=True, append=False) as writer:
                        for row in filtered.to_records():
                            writer.add(row[0], row["open"], row["high"], row["low"], row["close"], row["volume"], row["openInterest"])
                        writer.save()
                        filtered_data = True

        if not filtered_data:
            with data.get_writer(tags, 0, 0, create=True, append=False) as writer:
                writer.save()
             

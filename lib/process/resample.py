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
        return self._generate_multi()
 

    def run (self, queued, progress):
        print("resample %s" % (queued))
        Process.impl(in_series_id=queued.depend[0], add=self._add, remove=self._remove, period=self._params["period"])


    @staticmethod
    def impl (in_series_id, add, remove, period):
        series = schema.select_one("series", schema.table.series.id==in_series_id)
        tags = data.decode_tags(series.tags)
        for name, value in add.items():
            tags[name] = value
        for name, value in remove.items():
            if name in tags:
                del[name]
        write_empty = True

        if series.count > 0:
            wrapper = DataFrameWrapper(series)
            ohlc_sampler = { "open":"first"
                          , "high":"max"
                          , "low":"min"
                          , "close":"last"
                          , "volume":"sum"
                          , "openInterest":"sum"
                          , "actual":"first" }
            resampled = wrapper.dframe.resample(period, how=ohlc_sampler)
            resampled = resampled.dropna(how='all')
            if len(resampled) > 0:
                first = resampled.head(1).index[0].value
                last = resampled.tail(1).index[0].value
                with data.get_writer(tags, first, last, create=True, append=False) as writer:
                    for row in resampled.to_records():
                        writer.add( row[0]
                                  , row["open"]
                                  , row["high"]
                                  , row["low"]
                                  , row["close"]
                                  , row["volume"]
                                  , row["openInterest"]
                                  , row["actual"] )
                    writer.save()
                    write_empty = False

        if write_empty:
            with data.get_writer(tags, 0, 0, create=True, append=False) as writer:
                writer.save()
             

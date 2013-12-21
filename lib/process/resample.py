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
 
   
    def run (self, queued):
        series = schema.select_one("series", schema.table.series.id==queued.depend[0])
        wrapper = DataFrameWrapper(series)
        ohlc_sampler = { "open":"first"
                      , "high":"max"
                      , "low":"min"
                      , "close":"last"
                      , "volume":"sum"
                      , "openInterest":"sum"
                      , "actual":"first" }
        resampled = wrapper.dframe.resample(self._params["period"], how=ohlc_sampler)
        tags = data.decode_tags(series.tags)
        for name, value in self._add.items():
            tags[name] = value
        for name, value in self._remove.items():
            if name in tags:
                del[name]
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

        else:
            with data.get_writer(tags, 0, 0, create=True, append=False) as writer:
                writer.save()
             

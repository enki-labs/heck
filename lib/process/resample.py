
from lib.process import DataFrameWrapper
from lib import data

class Process (object):

    @staticmethod
    def parameters ():
        return dict(period="string")

    @staticmethod
    def run (series, params, output):
        wrapper = DataFrameWrapper(series)
        ohlc_sampler = { "open":"first"
                      , "high":"max"
                      , "low":"min"
                      , "close":"last"
                      , "volume":"sum"
                      , "openInterest":"sum"
                      , "actual":"first" }
        resampled = wrapper.dframe.resample(params["period"], how=ohlc_sampler)
        if len(resampled) > 0:
            tags = data.decode_tags(series.tags)
            for name, value in output["add"].items():
                tags[name] = value
            for name, value in output["remove"].items():
                if name in tags:
                    del[name]
            first = resampled.head(1).index[0].value
            last = resampled.tail(1).index[0].value
            writer = data.get_writer(tags, first, last, create=True, append=False)
            for row in resampled.to_records():
                writer.add( row[0]
                          , row["open"]
                          , row["high"]
                          , row["low"]
                          , row["close"]
                          , row["volume"]
                          , row["openInterest"]
                          , row["actual"] )
            writer.close()


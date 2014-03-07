import numpy as np
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
        print(queued)
        series = schema.select_one("series", schema.table.series.id==queued.depend[0])
        resampled = False

        with data.get_reader_pandas(series) as reader:
            ohlc_sampler = { "price": ["first", "max", "min", "last"],
                         "volume": "sum" }
            resampled_out = reader.df[reader.df["filter_class"]==""].resample(self._params["period"], how=ohlc_sampler).dropna()
            #TODO work out how to rename these columns
            #resampled.rename(None, lambda ct: "_".join(ct), False, True)
            resampled = True

        tags = data.decode_tags(series.tags)
        for name, value in self._add.items():
            tags[name] = value
        for name, value in self._remove.items():
            if name in tags:
                del[name]
        if resampled and len(resampled_out) > 0:
            first = resampled_out.head(1).index[0].value
            last = resampled_out.tail(1).index[0].value
            with data.get_writer(tags, first, last, create=True, append=False) as writer:
                #TODO work out how to index these f-ing columns.   print(type(list(resampled_out.columns.values)[0]))
                for row in resampled_out.to_records():
                    writer.add( row[0]
                              , row[2]#o
                              , row[3]#h
                              , row[4]#l
                              , row[5]#c
                              , row[1]#volume
                              , 0
                              , 0 )
                writer.save()

        else:
            with data.get_writer(tags, 0, 0, create=True, append=False) as writer:
                writer.save()
             

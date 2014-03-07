
import io
import re
import yaml
import pytz
import datetime
from bunch import Bunch
import numpy as np
from lib import common
from lib import data
from lib.data import tick
from lib.process import ProcessBase
from lib import schema


class Process (ProcessBase):


    def __init__ (self, process_def):
        super().__init__(process_def)


    def parameters (self):
        return dict(config="string")


    def generate (self):
        return self._generate_multi()


    @staticmethod
    def clone_row (row):
        return dict( time=row["time"]
                   , event=row["event"]
                   , price=row["price"]
                   , volume=row["volume"]
                   , qualifier=row["qualifier"]
                   , acc_volume=row["acc_volume"])



    def run (self, queued, progress):
        series = schema.select_one("series", schema.table.series.id==queued.depend[0])
        tags = queued.tags_dict
        #config = json.loads(self.get_config(tags).content)

        with data.get_reader(series) as reader:
            with data.get_writer(tags, 0, 0, create=True, append=False) as writer:
 
                progress.progress_end(reader._table.nrows)

                for raw_row in reader._table: #TODO read_iter not working?
                    progress.progress()
                    writer.add_row(row)
                    writer.save()


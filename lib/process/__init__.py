import pandas as pd
import numpy as np
import pytz
from pandas.lib import Timestamp
from lib import data


class DataFrameWrapper (object):
    """
    Opens a data series and reads into a dataframe.
    """

    def __init__ (self, series):
        self.series = series
        with data.get_reader(series) as reader:
            datatable = reader.read()
            self.dframe = pd.DataFrame.from_records(
                         datatable 
                       , index=pd.DatetimeIndex(
                               pd.Series(datatable["time"]).astype("datetime64[ns]")
                       , tz="UTC"))



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
        self.reader = data.get_reader(series)
        self.data = self.reader.read()
        self.dframe = pd.DataFrame.from_records(
                         self.data
                       , index=pd.DatetimeIndex(
                               pd.Series(self.data["time"]).astype("datetime64[ns]")
                       , tz="UTC"))



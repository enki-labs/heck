import pandas as pd
import numpy as np
import pytz
from pandas.lib import Timestamp
from sqlalchemy import and_, not_
from lib import data
from lib import schema
import exception


def get (process):
    """ Get a process instance """
    process_lib = __import__("lib.process." + process.processor, globals(), locals(), ["Process"])
    cls = getattr(process_lib, "Process")
    return cls(process)


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



class ProcessBase (object):
    """
    Data processing base class and utility functions.
    """

    def __init__ (self, process):
        """ Set underlying process definition data """
        self._process = process
        self._include = self._process.search_dict["include"]
        self._exclude = self._process.search_dict["exclude"]
        self._add = self._process.output_dict["add"]
        self._remove = self._process.output_dict["remove"]
        self._params = self._process.output_dict["params"]
        self._config_keys = None
        self._celery_id = None

        if "config" in self._params:
            config = self._params["config"]
            if "match" in config:
                self._config_keys = config["match"]

    @property
    def id (self):
        return self._process.id

    def set_celery (self, celery_id, backend):
        """ Set the Celery backend for progress reporting """
        self._celery_id = celery_id
        self._celery_backend = backend

    def progress (self, current, end):
        """ Report progress """
        if self._celery_id:
            self._celery_backend.store_result(self._celery_id,
                      result={"step": current, "total": end},
                      status="PROGRESS")

    def get_config (self, tags):
        """ Return a config based on the process target tags
            and config key """
        if self._config_keys == None:
            raise exception.NoConfigKeyException()
        
        config_tags = {}
        for key in self._config_keys:
            config_tags[key] = tags[key]

        config_tags_ids = data.resolve_tags(config_tags, create=False)
        config = schema.select_one("config", schema.table.config.tags==config_tags_ids)
        if config == None:
            raise exception.NoConfigException(tags, self._config_keys)
        return config.content                

    def _generate_multi (self):
        """ Generate series on one in one out basis """
        include_tags_ids = data.resolve_tags(self._include, create=False)
        exclude_tags_ids = data.resolve_tags(self._exclude, create=False)
        series = schema.table.series
        outputs = []
        with schema.select("series"
                         , series.tags.contains(include_tags_ids)
                         #, not_(series.tags.contains(exclude_tags_ids))
                         ) as select:
            for selected in select.all():
                tags = data.decode_tags(selected.tags)
                for name, value in self._remove.items():
                    if name in tags:
                        del[name]
                for name, value in self._add.items():
                    tags[name] = value
                outputs.append(dict(last_modified=selected.last_modified, tags=tags, depend=[selected.id]))
        return outputs


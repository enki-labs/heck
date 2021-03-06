
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


class EventFilter (object):
    """ Filter based on the tick event type """

    def __init__ (self, event):
        self._event = tick.TickEventEnum().lookup(event)

    def filter (self, row):
        if row["event"] == self._event:
            return ("event", "%s != %s" % (row["event"], self._event))
        else: return None


class NanFilter (object):
    """ Filter ticks where the given column is of type NAN """

    def __init__ (self, column):
        self._column = column

    def filter (self, row):
        if np.isnan(row[self._column]):
            return ("nan", "%s is nan" % (self._column))
        else: return None


class WeekendFilter (object):
    """ Filter ticks within the weekend boundary of a given timezone and market """

    def __init__ (self, timezone=None, weekend=None, weekstart=None):
        self._timezone = pytz.timezone(timezone)
        week_ordinal_map = dict(Monday=0, Tuesday=1, Wednesday=2, Thursday=3, Friday=4, Saturday=5, Sunday=6)
        self._weekend_day = week_ordinal_map[weekend.split(" ")[0]]
        self._weekstart_day = week_ordinal_map[weekstart.split(" ")[0]]
        self._weekend_time = datetime.datetime.strptime(weekend, "%A %H:%M").time()
        self._weekstart_time = datetime.datetime.strptime(weekstart, "%A %H:%M").time()
        self._next_enter_filter = None
        self._next_exit_filter = None

    def filter (self, row):
        time = row["time"]
        filter_tick = False

        if self._next_enter_filter == None and self._next_exit_filter == None:
            self._update_enter_exit(time)

        if self._next_enter_filter and self._next_exit_filter:
            if time < self._next_enter_filter:
                filter_tick = False
            elif time > self._next_enter_filter and time < self._next_exit_filter:
                filter_tick = True
            else:
                self._next_enter_filter = None
                self._next_exit_filter = None
                self._update_enter_exit(common.Time.tick(common.Time.time(time) + datetime.timedelta(1))) #bump
                filter_tick = False

        if filter_tick:
            return ("weekend", "")
        else: return None 
               
    def _update_enter_exit (self, time):
        localtime = common.Time.time(time).astimezone(self._timezone)
        
        if self._next_enter_filter == None:
            localenter = localtime
            while self._next_enter_filter == None:
                while localenter.weekday() != self._weekend_day:
                    localenter += datetime.timedelta(1)
                localenter = localenter.replace(hour=self._weekend_time.hour, minute=self._weekend_time.minute, second=0, microsecond=0)
                self._next_enter_filter = common.Time.tick(localenter.astimezone(pytz.utc))

        if self._next_exit_filter == None:
            localexit = localtime
            while self._next_exit_filter == None:
                while localexit.weekday() != self._weekstart_day:
                    localexit += datetime.timedelta(1)
                localexit = localexit.replace(hour=self._weekstart_time.hour, minute=self._weekstart_time.minute, second=0, microsecond=0)
                self._next_exit_filter = common.Time.tick(localexit.astimezone(pytz.utc))


class QualifierFilter (object):
    """ Filter ticks based on the string qualifiers """

    def __init__ (self, match):
        self._match = re.compile(match)
        self._match_string = match

    def filter (self, row):
        if self._match.match(row["qualifier"].decode("utf-8")):
            return ("qualifier", "%s" % (self._match_string))
        else: return None

class FloorFilter (object):
    """ Filter based on a lower boundary """

    def __init__ (self, min_value, column):
        self._min_value = min_value
        self._column = column

    def filter (self, row):
        if row[self._column] < self._min_value:
            return ("floor", "%s < %s" % (self._column, self._min_value))
        else: return None


class CapFilter (object):
    """ Filter based on an upper boundary """

    def __init__ (self, max_value, column):
        self._max_value = max_value
        self._column = column

    def filter (self, row):
        if row[self._column] > self._max_value:
            return ("cap", "%s > %s" % (self._column, self._max_value))
        else: return None


class StepFilter (object):
    """ Filter based on a bounded tick-to-tick step. Where the two tick
        times are more than a given number of seconds apart the step
        will be ignored. Repeated failed ticks will only be processed
        up to a given maximum count after which the previous tick resets """

    def __init__ (self, timeout_seconds, max_step, max_count, column):
        self._filter_count = 0
        self._column = column
        self._timeout = common.Time.nano_per_second * timeout_seconds
        self._max_step = max_step
        self._max_count = max_count
        self._last_time = None
        self._last_value = None
        self._filter_desc = "(%s) Column(%s)" % (max_step, column)

    def filter (self, row):

        filtered = False
        value = row[self._column]
        time = row["time"]

        if (    ( self._last_time and self._last_value )
           and  ( abs(value - self._last_value) >= self._max_step )
           and  ( (time - self._last_time) <= self._timeout )
           and  ( self._filter_count < self._max_count ) ):
            self._filter_count += 1
            filtered = True
        else:
            self._last_time = time
            self._last_value = value
            self._filter_count = 0
            filtered = False

        if filtered:
            return ("step", self._filter_desc)
        else: return None    
            


class Config (object):
    """ Parse a filter config in YAML format """

    def __init__ (self, config_yaml):
        """ Parse a yaml configuration string """
        configdef = yaml.safe_load(io.StringIO(config_yaml))         

        if "filters" not in configdef:
            configdef = dict(filters=[configdef])

        self._configs = []

        for definition in configdef["filters"]:
            config = Bunch( valid_from = None
                         , volume_follows = False
                         , copy_last_price = False
                         , copy_last_volume = False
                         , qualifier_include_filters = []
                         , qualifier_exclude_filters = []
                         , exclude_filters = [] )

            if "filter" in definition and definition["filter"] != None:
                for exclude_filter in definition["filter"]:
                    parts = exclude_filter.split(",")
                    if parts[0] == "floor":
                        config.exclude_filters.append(FloorFilter(float(parts[1]), "price"))
                    elif parts[0] == "cap":
                        config.exclude_filters.append(CapFilter(float(parts[1]), "price"))    
                    elif parts[0] == "step":
                        config.exclude_filters.append(StepFilter(int(parts[1]), float(parts[2]), float(parts[3]), "price"))
                    else:
                        raise Exception("Unknown filter (%s)" % (parts[0]))   
            
            if "remove" in definition and definition["remove"] != None:
                for exclude_filter in definition["remove"]:
                    config.qualifier_exclude_filters.append(QualifierFilter(exclude_filter))
            
            if "allow" in definition and definition["allow"] != None:
                for include_filter in definition["allow"]:
                    config.qualifier_include_filters.append(QualifierFilter(include_filter))

            if "volFollows" in definition: config.volume_follows = definition["volFollows"] 
            if "copyLast" in definition and definition["copyLast"] != None:
                config.copy_last_price = definition["copyLast"] 
                config.copy_last_volume = definition["copyLast"] 
            if "volumeLimit" in definition and definition["volumeLimit"] != None:
                config.exclude_filters.append(CapFilter(definition["volumeLimit"], "volume"))
            if "validFrom" in definition and definition["validFrom"] != None:
                valid_from = datetime.datetime.strptime(definition["validFrom"], "%Y-%m-%d %H:%M:%S")
                valid_from.replace(tzinfo=pytz.utc)
                config.valid_from = common.Time.tick(valid_from)
            if "weekTimezone" in definition and definition["weekTimezone"] != None:
                config.exclude_filters.append(WeekendFilter(definition["weekTimezone"], definition["weekEnd"], definition["weekStart"]))

            self._configs.append(config)
        
        self._config_index = 0
        self._config_count = len(self._configs)

    def get (self):
        """ Return the next config and the time for the next config change """
        next_config = self._configs[self._config_index]
        self._config_index += 1
        if self._config_index < self._config_count:
            return (next_config, self._configs[self._config_index].valid_from)
        else:
            return (next_config, None)
    

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
        #tags = data.decode_tags(series.tags)
        #for name, value in self._add.items():
        #    tags[name] = value
        #for name, value in self._remove.items():
        #    if name in tags:
        #        del tags[name]

        #configs = Config(self._config)
        configs = Config(self.get_config(tags).content)

        with data.get_reader(series) as reader:
            with data.get_writer(tags, 0, 0, create=True, append=False) as writer:

                config, next_config = configs.get()
                event_filter = EventFilter("trade")
                price_filter = NanFilter("price")
                volume_filter = NanFilter("volume")
                last_row = None
                progress.progress_end(reader._table.nrows)

                for raw_row in reader._table: #TODO read_iter not working?
                    progress.progress()
                    row = Process.clone_row(raw_row)
                    if not event_filter.filter(row): continue
                    if last_row == None: last_row = row

                    if next_config and row["time"] > next_config:
                        config, next_config = configs.get()

                    if config.copy_last_price and np.isnan(row["price"]):
                        row["price"] = last_row["price"]
                    if config.copy_last_volume and np.isnan(row["volume"]):
                        row["volume"] = last_row["volume"]

                    nanprice = price_filter.filter(row)
                    if nanprice:
                        row["filter_class"] = nanprice[0]
                        row["filter_detail"] = nanprice[1]
                        writer.add_row(row)
                        continue 
                    nanvolume = volume_filter.filter(row)
                    if nanvolume:
                        row["filter_class"] = nanvolume[0]    
                        row["filter_detail"] = nanvolume[1]
                        writer.add_row(row)
                        continue

                    #TODO handle volume on next row

                    #print(common.Time.time(row["time"]), row["price"], row["volume"], row["event"], row["qualifier"], row["acc_volume"])

                    filter_reason = None
                    write_row = False

                    # filter qualifiers
                    # include on first match
                    for include_filter in config.qualifier_include_filters:
                        if include_filter.filter(row) != None:
                            write_row = True
                            break

                    if not write_row:
                        # exclude on first match
                        write_row = True
                        for exclude_filter in config.qualifier_exclude_filters:
                            filter_reason = exclude_filter.filter(row)
                            if filter_reason:
                                write_row = False
                                break

                    if write_row:
                        # exclude on first match
                        for exclude_filter in config.exclude_filters:
                            filter_reason = exclude_filter.filter(row)
                            if filter_reason:
                                write_row = False
                                break

                    if write_row:
                        row["filter_class"] = ""
                        row["filter_detail"] = ""
                        writer.add_row(row)
                    else:
                        row["filter_class"] = filter_reason[0]    
                        row["filter_detail"] = filter_reason[1]
                        writer.add_row(row)

                    last_row = row

                writer.save()


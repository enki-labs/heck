
import re
import yaml
import pytz
import datetime
from lib import data


class WeekendFilter (object):

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
            elif time < self._next_exit_filter:
                filter_tick = True
                self._next_enter_filter = None
                self._update_enter_exit(time)
            else:
                self._next_exit_filter = None
                self._update_enter_exit(time)
        
        return filter_tick 
               
    def _update_enter_exit (self, time):
        localtime = common.Time.time(time).astimezone(self._timezone)
        
        if self._next_enter_filter == None:
            localenter = localtime
            while localenter.weekday() != self._weekend_day:
                localenter += datetime.datetime.timedelta(1)
            localenter += self._weekend_time
            self._next_enter_filter = common.Time.tick(localenter.astimezone(pytz.utc))

        if self._next_exit_filter == None:
            localexit = localtime
            while localexit.weekday() != self._weekstart_day:
                localexit += datetime.datetime.timedelta(1)
            localexit += self._weekstart_time
            self._next_exit_filter = common.Time.tick(localexit.astimezone(pytz.utc))


class QualifierFilter (object):

    def __init__ (self, match):
        self._match = re.compile(match)

    def filter (self, row):
        return self._match.match(row["qualifier"])


class FloorFilter (object):

    def __init__ (self, min_value, column):
        self._min_value = min_value
        self._column = column

    def filter (self, row):
        if row[self._column] < self._value:
            return True
        else:
            return False


class CapFilter (object):

    def __init__ (self, max_value, column):
        self._max_value = max_value
        self._column = column

    def filter (self, row):
        if row[self._column] > self._max_value:
            return True
        else:
            return False


class StepFilter (object):

    def __init__ (self, timeout_seconds, max_step, max_count, column):
        self._filter_count = 0
        self._column = column
        self._timeout = common.Time.nano_per_second * timeout_seconds
        self._max_step = max_step
        self._max_count = max_count
        self._last_time = None
        self._last_value = None

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

        if (( self._lastTime and self._lastPrice )
           and   ( abs(price - self._lastPrice) >= self._maxPriceStep )
           and   ( (time - self._lastTime) <= self._timeout )
           and   ( self._filterCount < self._maxFilterCount ) ):
            self._filterCount += 1
            filtered = True

        return filtered


class Config (object):

    def __init__ (self, config_yaml):
        """ Parse a yaml configuration string """
        configdef = yaml.safe_load(io.StringIO(config_yaml))         
        self._configs = []

        for definition in configdef["filters"]:

            config = dict( valid_from = None
                         , volume_follows = False
                         , copy_last_price = False
                         , copy_last_volume = False
                         , include_filters = []
                         , exclude_filters = [] )

            if "filter" in definition:
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
            
            if "remove" in definition:
                for exclude_filter in definition["remove"]:
                    config.exclude_filters.append(QualifierFilter(exclude_filter))
            
            if "allow" in definition:
                for include_filter in definition["allow"]:
                    config.include_filters.append(QualifierFilter(include_filter))

            if "volFollows" in definition: config.volume_follows = definition["volVollows"] 
            if "copyLast" in definition:
                config.copy_last_price = definition["copyLast"] 
                config.copy_last_volume = definition["copyLast"] 
            if "volumeLimit" in definition:
                config.exclude_filters.append(CapFilter(definition["volumeLimit"], "volume"))
            if "validFrom" in definition:
                valid_from = definition["validFrom"]
                valid_from.replace(tzinfo=pytz.utc)
                config.valid_from = common.Time.tick(valid_from)
            if "weekTimezone" in definition:
                config.exclude_filters.append(TimeFilter(definition["weekTimezone"], definition["weekEnd"], definition["weekStart"]))

            self._configs.append(config)
        
        self._config_index = 0
        self._config_count = len(self._configs)

    def get (self):
        """ Return the next config and the time for the next config change """
        next_config = self._configs[self._config_index]
        self._config_index += 1
        if self._config_index < self._config_count:
            return (next_config, self._config[self._config_index].valid_from)
        else:
            return (next_config, None)
    

class Process (object):

    @staticmethod
    def parameters ():
        return dict(filters="string")

    @staticmethod
    def run (series, params, output):

        tags = data.decode_tags(series.tags)
        for name, value in output["add"].items():
            tags[name] = value
        for name, value in output["remove"].items():
            if name in tags:
                del[name]

        # parse configuration(s)
        config = dict(
        volume_follows = False
        copy_last_price = False
        copy_last_volume = False
        include_filters = []
        exclude_filters = [] )
        configs = 

        with data.get_reader(series) as reader:
            with data.get_writer(tags, first, last, create=True, append=False):

                config, next_config = Config.get()
                last_row = None
                
                for row in reader.read_iter():
                    
                    if last_row == None: last_row = row

                    if next_config and row["time"] > next_config:
                        config, next_config = Config.get()

                    if config.copy_last_price and row["price"] == None:
                        row["price"] = last_row["price"]
                    if config.copy_last_volume and row["volume"] == None:
                        row["volume"] = last_row["volume"]
                    #todo handle volume on next row

                    write_row = False
                    # include on first match
                    for include in config.include_filters:
                        if include.filter(row):
                            write_row = True
                            break

                    if not write_row:
                        # exclude on first match
                        write_row = True
                        for exclude in config.exclude_filters:
                            if exclude.filter(row):
                                break

                    if write_row:
                        writer.add_row(row)
                    else:
                        pass #todo write filtered output



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


class EventFilter (object):

    def __init__ (self, event):
        self._event = tick.TickEventEnum().lookup(event)

    def filter (self, row, log):
        if row["event"] == self._event:
            log.filter("Event", "%s != %s" % (row["event"], self._event), row)
            return True
        else: return False


class NanFilter (object):

    def __init__ (self, column):
        self._column = column

    def filter (self, row, log):
        if np.isnan(row[self._column]):
            log.filter("NAN", "%s is nan" % (self._column), row)
            return True
        else: return False


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

    def filter (self, row, log):
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
            log.filter("Weekend", "", row)
            return True
        else: return False 
               
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

    def __init__ (self, match):
        self._match = re.compile(match)
        self._match_string = match

    def filter (self, row, log):
        if self._match.match(row["qualifier"].decode("utf-8")):
            log.filter("Qualifier", "%s" % (self._match_string), row)
            return True
        else: return False

class FloorFilter (object):

    def __init__ (self, min_value, column):
        self._min_value = min_value
        self._column = column

    def filter (self, row, log):
        if row[self._column] < self._min_value:
            log.filter("Floor", "%s < %s" % (self._column, self._min_value), row)
            return True
        else: return False


class CapFilter (object):

    def __init__ (self, max_value, column):
        self._max_value = max_value
        self._column = column

    def filter (self, row, log):
        if row[self._column] > self._max_value:
            log.filter("Cap", "%s > %s" % (self._column, self._max_value), row)
            return True
        else: return False


class StepFilter (object):

    def __init__ (self, timeout_seconds, max_step, max_count, column):
        self._filter_count = 0
        self._column = column
        self._timeout = common.Time.nano_per_second * timeout_seconds
        self._max_step = max_step
        self._max_count = max_count
        self._last_time = None
        self._last_value = None
        self._filter_desc = "(%s) Column(%s)" % (max_step, column)

    def filter (self, row, log):

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
            log.filter("Step", self._filter_desc, row)
        return filtered    
            


class Config (object):

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

            if "volFollows" in definition: config.volume_follows = definition["volFollows"] 
            if "copyLast" in definition:
                config.copy_last_price = definition["copyLast"] 
                config.copy_last_volume = definition["copyLast"] 
            if "volumeLimit" in definition:
                config.exclude_filters.append(CapFilter(definition["volumeLimit"], "volume"))
            if "validFrom" in definition:
                valid_from = definition["validFrom"]
                if valid_from != None:
                    valid_from.replace(tzinfo=pytz.utc)
                    config.valid_from = common.Time.tick(valid_from)
            if "weekTimezone" in definition:
                config.exclude_filters.append(WeekendFilter(definition["weekTimezone"], definition["weekEnd"], definition["weekStart"]))

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
    

class FilterLog (object):

    def __init__ (self, show):
        self._show = show
        self.summary = dict()

    def filter (self, typ, reason, row):
        if typ in self.summary:
            self.summary[typ] += 1
        else:
            self.summary[typ] = 1

        if self._show:
            #print(reason, common.Time.time(row["time"]), row["price"], row["volume"], row["event"], row["qualifier"], row["acc_volume"])
            pass


class Process (object):

    @staticmethod
    def parameters ():
        return dict(config="string")

    @staticmethod
    def clone_row (row):
        return dict( time=row["time"]
                   , event=row["event"]
                   , price=row["price"]
                   , volume=row["volume"]
                   , qualifier=row["qualifier"]
                   , acc_volume=row["acc_volume"])

    @staticmethod
    def run (series, params, output):

        tags = data.decode_tags(series.tags)
        for name, value in output["add"].items():
            tags[name] = value
        for name, value in output["remove"].items():
            if name in tags:
                del[name]

        configs = Config(params["config"])
        include = FilterLog(False)
        exclude = FilterLog(True)

        with data.get_reader(series) as reader:
            with data.get_writer(tags, 0, 0, create=True, append=False) as writer:

                config, next_config = configs.get()
                event_filter = EventFilter("trade")
                price_filter = NanFilter("price")
                volume_filter = NanFilter("volume")
                last_row = None

                for row in reader._table: #TODO read_iter not working?

                    if not event_filter.filter(row, include): continue
                    if last_row == None: last_row = Process.clone_row(row)

                    if next_config and row["time"] > next_config:
                        config, next_config = Config.get()

                    row_copy = False 
                    if config.copy_last_price and np.isnan(row["price"]):
                        if not row_copy:
                            row = Process.clone_row(row)
                            row_copy = True
                        row["price"] = last_row["price"]
                    if config.copy_last_volume and np.isnan(row["volume"]):
                        if not row_copy:
                            row = Process.clone_row(row)
                            row_copy = True
                        row["volume"] = last_row["volume"]

                    if price_filter.filter(row, exclude): continue
                    if volume_filter.filter(row, exclude): continue

                    #TODO handle volume on next row

                    #print(common.Time.time(row["time"]), row["price"], row["volume"], row["event"], row["qualifier"], row["acc_volume"])

                    write_row = False
                    # include on first match
                    for include_filter in config.include_filters:
                        if include_filter.filter(row, include):
                            write_row = True
                            break

                    if not write_row:
                        # exclude on first match
                        write_row = True
                        for exclude_filter in config.exclude_filters:
                            if exclude_filter.filter(row, exclude):
                                write_row = False
                                break

                    if write_row:
                        writer.add_row(row)
                    else:
                        #print("- ", common.Time.time(row["time"]), row["price"], row["volume"], row["event"], row["qualifier"], row["acc_volume"])
                        pass #todo write filtered output

                    last_row = Process.clone_row(row)

                writer.save()
                print(exclude.summary) 


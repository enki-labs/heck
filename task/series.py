from celery.decorators import task
import os
import sys
sys.path.append(os.getcwd())
from task import init_task
import exception 


@task (bind=True)
@init_task
def check (self):

    with self.__t.steps():
        import exception
        from lib import schema
        from lib import common
        from task import process
        from lib import data
        select_tags = data.resolve_tags([{"period": "1min"}, {"period": "5min"}, {"period": "60min"}, {"period": "1day"}], create=False)
        with schema.select("series", schema.table.series.start==-1, schema.table.series.end==-1, schema.table.series.tags.overlap(select_tags)) as series_query:
            for series in series_query.all():
                tags = data.decode_tags(series.tags)
                if tags["format"] == "ohlc" and "period" in tags:
                    if tags["period"] in ["1min", "5min", "60min", "1day"]:
                        lookup = {"1min": ("1sec",13), "5min": ("1min",14), "60min": ("5min",15), "1day": ("5min",16)}
                        out_period = tags["period"]
                        sample_from = lookup[out_period][0]
                        in_tags = tags.copy()
                        in_tags["period"] = sample_from
                        in_series = schema.select_one("series", schema.table.series.tags==data.resolve_tags(in_tags, create=False))
                        if in_series:
                            #common.log.info("Initialising series %s" % series)
                            process.run_new.apply_async((lookup[out_period][1], in_series.id, tags))


@task (bind=True)
@init_task
def check_time (self):

    with self.__t.steps():
        import exception
        from lib import schema
        from lib import common
        from task import process
        from lib import data
        select_tags = data.resolve_tags([{"period": "1day"}, {"provider": "bloomberg"}], create=False)
        overlap_tags = data.resolve_tags([{"year": "*"}, {"month": "*"}], create=False)
        with schema.select("series", schema.table.series.tags.contains(select_tags), schema.table.series.tags.overlap(overlap_tags)) as series_query:
            for series in series_query.all():
                repair_time.apply_async((series.id,))
    

@task (bind=True)
@init_task
def repair_time (self, series_id):
    with self.__t.steps():
        import exception
        from lib import schema
        from lib import common
        from task import process
        from lib import data
        from lib.process import DataFrameWrapper
        from datetime import timedelta
        import os
        os.system("cp /home/data/data/store/ohlc/%s /home/data/data/store/ohlc/%s_bak" % (series_id, series_id)) 
        series = schema.select_one("series", schema.table.series.id == series_id)
        reader = DataFrameWrapper(series)
        last_time = None
        with data.get_writer(data.decode_tags(series.tags), None, None, create=True, append=False) as writer:
            if reader.dframe:
                for row in reader.dframe.to_records():
                    this_time = row[0]
                    if this_time.hour == 23: this_time += timedelta(hours=1)
                    if last_time != None and last_time == this_time: continue
                    last_time = this_time
                    writer.add( this_time
                          , row["open"]
                          , row["high"]
                          , row["low"]
                          , row["close"]
                          , row["volume"]
                          , row["openInterest"]
                          , row["actual"] )
                writer.save()
            else:
                print("NO DATA")


@task (bind=True)
@init_task
def generate (self):
    """ Check process definitions and queue tasks """

    with self.__t.steps():
        import exception
        from lib import schema
        from lib import common
        from lib import process
        from lib import data
        from sqlalchemy import and_

        compute_tags = data.resolve_tags({"compute": "*"}, create=False)
        compute_series = {}

        for compute_tag in compute_tags:
            with schema.select("config", schema.table.config.tags.contains([compute_tag])) as configs:
                for config_item in configs.all():
                    search_tags = [x for x in config_item.tags if x not in compute_tags]
                    with schema.select("series", schema.table.series.tags.contains(search_tags)) as series:
                        for series_item in series.all():
                            series_tags = data.decode_tags(series_item.tags)
                            try:
                                series_tags.pop("year", None)
                                series_tags.pop("month", None)
                                compute_series["%s_%s" % (compute_tag, str(series_tags))] = {"compute": compute_tag, "series": series_tags}
                            except KeyError:
                                pass

        for series_def in compute_series.values():
            tags = data.resolve_tags(series_def["series"], create=False)
            tags.append(series_def["compute"])
            series = schema.select_one("series", schema.table.series.tags==tags)
            if not series:
                series = schema.table.series()
                series.tags = tags
                series.symbol = series_def["series"]["symbol"]
                series.start = 0
                series.end = 0
                series.count = 0
                series.last_modified = -1
                schema.save(series)

        self.__t.ok()



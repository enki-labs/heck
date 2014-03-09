from celery.decorators import task
import os
import sys
sys.path.append(os.getcwd())
from task import init_task
import exception 


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



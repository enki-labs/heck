import time
from celery.decorators import task
from decorator import decorator
from itertools import chain
import traceback
import json
import os
import math
import sys
sys.path.append(os.getcwd())

#TODO work out how to decorate the init_task decorator with @task(bind=True)
@decorator
def init_task (f, *args, **kw):
    """
    Import the project base path and inject
    test backend into class instance.
    """

    import os
    import sys
    sys.path.append(os.getcwd())
    from config import prod

    args[0].__t = BackendStore(args[0].request.id, args[0].name)
    return f(*args, **kw)


@decorator
def lock_task (f, *args, **kw):
    import exception
    from lib import common
    try:
        lockname = "task_%s.%s" % (__name__, f.__name__)
        with common.ScopedResourceLock(lockname, timeout_secs=0.5) as lock:
            return f(*args, **kw)
    except exception.TimeoutException:
        pass


class TaskScope (object):

    def __init__ (self, backend):
        self._backend = backend

    def __enter__ (self):
        self._backend.start()

    def __exit__ (self, ex_type, ex_value, ex_traceback):
        if ex_type:
            self._backend.fail({"exception": traceback.format_exception(ex_type, ex_value, ex_traceback)})


"""
Task backend procedures.

"""
class BackendStore (object):

    def __init__ (self, task_id, task_name):
        self._progress_timeout = 0
        self._progress_current = 0
        self._progress_current_last = 0
        self._progress_end = 0
        self._db_entity = None

        from lib import schema
        from lib import common
        self._db_entity = schema.table.status()
        self._db_entity.task_id = task_id
        self._db_entity.node = "rabbit@Ubuntu-1204-precise-64-minimal"
        self._db_entity.name = task_name
        self._db_entity.status = "PENDING"
        self._db_entity.started = common.Time.tick()
        self._db_entity.last_modified = common.Time.tick()
        self._db_entity.result = "{}"
        schema.save(self._db_entity)


    def _write_db (self, status, result):
        from lib import schema
        from lib import common
        self._db_entity.status = status
        self._db_entity.last_modified = common.Time.tick()
        self._db_entity.result = json.dumps(result)
        schema.save(self._db_entity)


    def _write_progress (self, current, end, per_second):
        self._write_db("PROGRESS", {"step": current, "total": end, "per_second": per_second})


    def steps (self):
        return TaskScope(self)


    def progress (self):
        """ Update task progress. Updates are rate limited."""
        self._progress_current += 1
        progress_timeout = time.time()
        time_span = (progress_timeout - self._progress_timeout)
        if time_span > 20:
            per_second = math.ceil((self._progress_current - self._progress_current_last) / time_span)
            self._progress_current_last = self._progress_current
            self._write_progress(self._progress_current, self._progress_end, per_second)
            self._progress_timeout = progress_timeout


    def progress_end (self, end):
        self._progress_end = int(end)


    def progress_end_increment (self):
        self._progress_end += 1


    def name (self, name):
        from lib import common
        from lib import schema
        self._db_entity.name = name
        self._db_entity.last_modified = common.Time.tick()
        schema.save(self._db_entity)


    def start (self):
        self._write_db("START", {})


    def ok (self, value=None):
        progress_value = {"processed": self._progress_end}
        if value:
            self._write_db("SUCCESS", dict(chain(value.items(), progress_value.items())))
        else:
            self._write_db("SUCCESS", progress_value)


    def fail (self, value):
        progress_value = {"processed": self._progress_end}
        self._write_db("FAILURE", dict(chain(value.items(), progress_value.items())))

    

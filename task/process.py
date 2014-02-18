from celery.decorators import task
from decorator import decorator
import os
import sys
sys.path.append(os.getcwd())


@decorator
def fix_import_path (f, *args, **kw):
    import os
    import sys
    sys.path.append(os.getcwd())
    from config import prod
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


@task (ignore_result=True)
@fix_import_path
@lock_task
def generate ():
    """ Check process definitions and queue tasks """

    import exception
    from lib import schema
    from lib import common
    from lib import process
    from lib import data
    from sqlalchemy import and_

    def queue (process_id, generated):
        from sqlalchemy.exc import IntegrityError
        queued = schema.table.process_queue()
        queued.process = process_id
        queued.tags = generated["tags"]
        queued.depend = generated["depend"]
        try:
            schema.save(queued)
        except IntegrityError:
            pass

    output = ""
    with schema.select("process") as select:
        for process_def in select.all():
            if process_def.id != 7:
                continue
            proc = process.get(process_def)
            for generated in proc.generate():
                try:
                    with schema.select( "series"
                                      , schema.table.series.tags.contains(data.resolve_tags(generated["tags"], create=False))) as select2:
                        selected = select2.all()
                        if len(selected) > 0:
                            output += "%s < %s" % (selected[0].last_modified, generated["last_modified"])
                            if selected[0].id != generated["depend"] and selected[0].last_modified < generated["last_modified"]:
                                queue(proc.id, generated)
                        else:
                            queue(proc.id, generated)
                except exception.MissingTagException:
                    queue(proc.id, generated)
    return output


@task (ignore_result=True)
@fix_import_path
@lock_task
def queue ():
    """ Check dependencies and queue tasks """
    import exception
    from lib import schema
    from lib import common
    from lib import process
    from lib import data
    from sqlalchemy import and_
    import json
    from collections import OrderedDict

    count = 0
    with schema.select("process_queue", schema.table.process_queue.status==None) as select:
        for queued in select.all():
            blocked = False
            if len(queued.depend) > 0:
                for depend_id in queued.depend:
                    depend = schema.select_one("series", schema.table.series.id==depend_id)
                    match_tags = json.dumps(OrderedDict(sorted(data.decode_tags(depend.tags).items())))
                    if depend and schema.select_one("process_queue", schema.table.process_queue.tags==match_tags):
                        blocked = True
                        break # queued dependencies
            if not blocked:
                count += 1
                #if count > 200: break
                queued.status = "queued"
                schema.save(queued)
                run.delay(queued.tags)

@task
@fix_import_path
def run (tags, task_id=None):
    """ Run process """

    import exception
    from lib import schema
    from lib import common
    from lib import process
    from lib import data
    from sqlalchemy import and_
    import json
    from collections import OrderedDict

    result = None
    queued = schema.select_one("process_queue", schema.table.process_queue.tags==tags)
    if queued:
        proc_def = schema.select_one("process", schema.table.process.id==queued.process)
        if proc_def:
            proc = process.get(proc_def)
            proc.set_celery(task_id, run.backend)
            result = proc.run(queued)
        schema.delete(queued)

    return result

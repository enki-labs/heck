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

        with schema.select("process") as select:
            for process_def in select.all():
                if process_def.id != 4:
                    continue
                proc = process.get(process_def)
                for generated in proc.generate():
                    try:
                        with schema.select( "series"
                                      , schema.table.series.tags.contains(data.resolve_tags(generated["tags"], create=False))) as select2:
                            selected = select2.all()
                            if len(selected) > 0:
                                if selected[0].id != generated["depend"] and selected[0].last_modified < generated["last_modified"]:
                                    print("%s < %s" % (selected[0].last_modified, generated["last_modified"]))
                                    queue(proc.id, generated)
                            else:
                                queue(proc.id, generated)
                    except exception.MissingTagException:
                        queue(proc.id, generated)

        #self.__t.progress_end(30)
        #self.__t.progress()
        #self.apply_async(queue="control", countdown=30) #queue next
        self.__t.ok()


@task (bind=True)
@init_task
def queue (self):
    """ Check dependencies and queue tasks """

    with self.__t.steps():
        import exception
        from lib import schema
        from lib import common
        from lib import process
        from lib import data
        from sqlalchemy import and_
        import json
        from collections import OrderedDict

        with schema.select("process_queue", schema.table.process_queue.status==None) as select:
            for queued in select.limit(1000).all():
                blocked = False
                if len(queued.depend) > 0:
                    for depend_id in queued.depend:
                        depend = schema.select_one("series", schema.table.series.id==depend_id)
                        match_tags = json.dumps(OrderedDict(sorted(data.decode_tags(depend.tags).items())))
                        if depend and schema.select_one("process_queue", schema.table.process_queue.tags==match_tags):
                            blocked = True
                            break # queued dependencies
                if not blocked:
                    queued.status = "queued"
                    schema.save(queued)
                    run.apply_async([queued.tags]) #queue process
        self.__t.ok()
        self.apply_async(queue="control", countdown=30) #queue next


@task (bind=True)
@init_task
def run (self, tags):
    """ Run process """

    with self.__t.steps():
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
                self.__t.name(proc_def.name)
                proc = process.get(proc_def)
                result = proc.run(queued, self.__t)
            schema.delete(queued)

        self.__t.ok()



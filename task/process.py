from celery.decorators import task
import os
import sys
sys.path.append(os.getcwd())
from task import init_task
import exception 


@task (bind=True)
@init_task
def generate (self, options=None):
    """ Check process definitions and queue tasks """

    with self.__t.steps():
        import json
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
            queued.tags = generated["output_tags"]
            queued.depend = generated["output_depend"]
            try:
                schema.save(queued)
            except IntegrityError:
                pass

        with schema.select("process") as select:
            for process_def in select.all():

                if options and "process_id" in options:
                    if process_def.id != options["process_id"]: continue

                proc = process.get(process_def)
                for generated in proc.generate():
                    generate = False
                    print(generated)
                    try:
                        output_tags_ids = data.resolve_tags(generated["output_tags"], create=True)
                        generated_series = schema.select_one( "series"
                                      , schema.table.series.tags == output_tags_ids)
                        if generated_series:
                            generate = generated["input_last_modified"] > generated_series.last_modified
                            if not generate: #check modified config
                                try:
                                    config = proc.get_config(generated["output_tags"])
                                    generate = config.last_modified > generated_series.last_modified
                                except (exception.NoConfigException, exception.NoConfigKeyException):
                                    pass
                        else:
                            print("create")
                            generated_series = schema.table.series()
                            generated_series.symbol = generated["output_tags"]["symbol"]
                            generated_series.tags = output_tags_ids
                            schema.save(generated_series)
                            generate = True
                    except exception.MissingTagException:
                        generate = True
                    if generate or (options and "force" in options and options["force"]):
                        print("queue %s" % (generated))
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
            schema.delete(queued) #TODO should we delete this on error?

        self.__t.ok()

@task (bind=True)
@init_task
def run_new (self, process_id, depend_id, out_tags):
    with self.__t.steps():
        from bunch import Bunch
        from lib import schema
        from lib import process
        queued = Bunch(depend=[depend_id], tags_dict=out_tags)
        proc_def = schema.select_one("process", schema.table.process.id==process_id)
        if proc_def:
            self.__t.name(proc_def.name)
            proc = process.get(proc_def)
            result = proc.run(queued, self.__t)
        self.__t.ok()


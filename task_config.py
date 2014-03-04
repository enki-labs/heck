from datetime import timedelta

#CELERYD_FORCE_EXECV = True
CELERYD_FORCE_ROOT = True
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"
CELERY_ALWAYS_EAGER = False
#BROKER_POOL_LIMIT = None
#BROKER_URL = "amqp://celery:pass@localhost:5672/task"
BROKER_URL = "redis://:jdk9dD0knn8md922XkdWQ980D9dkSD90S809dk@5.9.111.76:6379/0"
BROKER_TRANSPORT_OPTIONS = { 'unacked_restore_limit': 0 }

CELERY_IMPORTS = ("task.inbound", "task.process")


#CELERYD_MAX_TASKS_PER_CHILD = 1 
#CELERY_ACKS_LATE = True
CELERY_REDIRECT_STDOUTS = False
CELERY_REDIRECT_STDOUTS_LEVEL = 'DEBUG'
CELERY_ENABLE_UTC = True
CELERY_TIMEZONE = 'UTC'
#CELERYD_POOL = "celery.concurrency.prefork:TaskPool"


CELERYD_CONCURRENCY = 6 
CELERYD_PREFETCH_MULTIPLIER = 1

CELERY_DISABLE_RATE_LIMITS=True
CELERY_SEND_EVENTS = True

CELERY_TRACK_STARTED = True

#CELERY_RESULT_BACKEND = "db+postgresql://heck:pass@localhost/heck"
#CELERY_RESULT_BACKEND = "database"
#CELERY_RESULT_DBURI = "postgresql://heck:pass@localhost/heck"

#CELERY_RESULT_DB_TABLENAMES = {
#    'task': 'celery_task',
#    'group': 'celery_group',
#}

"""
CELERYBEAT_SCHEDULE = {
    "inbound.update": {
        "task": "task.inbound.update",
        "schedule": timedelta(seconds=60),
        "args": None
    },
    "inbound.queue": {
        "task": "task.inbound.queue",
        "schedule": timedelta(seconds=60),
        "args": None
    },
    "process.generate": {
        "task": "task.process.generate",
        "schedule": timedelta(seconds=300),
        "args": None
    },
    "process.queue": {
        "task": "task.process.queue",
        "schedule": timedelta(seconds=60),
        "args": None
    }
}
"""


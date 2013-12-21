from datetime import timedelta

CELERYD_FORCE_EXECV = True
CELERY_ACCEPT_CONTENT = ["json"]
CELERY_TASK_SERIALIZER = "json"

BROKER_URL = "amqp://celery:pass@localhost:5672/task"

CELERY_IMPORTS = ("task.inbound", "task.process")

CELERYD_CONCURRENCY = 12

CELERYD_PREFETCH_MULTIPLIER = 1

CELERY_SEND_EVENTS = True

CELERY_TRACK_STARTED = True

CELERY_RESULT_BACKEND = "db+postgresql://heck:pass@localhost/heck"

CELERY_RESULT_DB_TABLENAMES = {
    'task': 'celery_task',
    'group': 'celery_group',
}

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


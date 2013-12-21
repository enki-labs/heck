#!/bin/bash
celery call task.inbound.update --config=task_config
celery call task.inbound.queue --config=task_config
celery call task.process.generate --config=task_config
celery call task.process.queue --config=task_config


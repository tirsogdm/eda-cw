import os
from celery import Celery

# RabbitMQ broker url scheme: amqp://USER:PASSWORD@HOSTNAME:PORT/VHOST
BROKER_URL = os.environ.get(
    "CELERY_BROKER_URL",
    "amqp://admin:abc123@controller-node:5672/protein"
)

RESULT_BACKEND = os.environ.get(
    "CELERY_RESULT_BACKEND",
    "redis://:imaproteinpipelinerunner@controller-node:6379/1"
)

app = Celery(
    "protein_pipeline",
    broker=BROKER_URL,
    backend=RESULT_BACKEND
)

app.conf.update(
    task_default_queue = "protein",
    task_track_started = True,
    result_expires = 3715200,
    task_create_missing_queues = True,
)

app.conf.task_routes = {
    "finalise_run": {"queue": "finalise"},
}
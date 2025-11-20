import os
from celery import Celery

# RabbitMQ broker url scheme: amqp://USER:PASSWORD@HOSTNAME:PORT/VHOST
BROKER_URL = os.environ.get(
    "CELERY_BROKER_URL",
    "amqp://admin:abc123@host-node:5672/protein"
)

RESULT_BACKEND = os.environ.get(
    "CELERY_RESULT_BACKEND",
    "rpc://"
)

app = Celery(
    "protein_pipeline",
    broker=BROKER_URL,
    backend=RESULT_BACKEND
)

app.conf.task_default_queue = "protein"
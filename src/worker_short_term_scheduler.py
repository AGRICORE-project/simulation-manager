import os
import ssl
import sys
from celery import Celery
from fastapi.encoders import jsonable_encoder
import sys
from api.tasks.task_manager import AgricoreCelery
from loguru import logger
import importlib.metadata
import agricore_sp_models as asp

QUEUE_SUFFIX = os.getenv('QUEUE_SUFFIX')
logger.info("Identified QUEUE_SUFFIX: {0}".format(QUEUE_SUFFIX))
logger.info(f"Agricore models library version: {importlib.metadata.version('agricore_sp_models')}")

# Celery APP configuration
app = Celery()
AgricoreCelery.set_config(app, QUEUE_SUFFIX)
AgricoreCelery.add_sp_scheduler_tasks(app)





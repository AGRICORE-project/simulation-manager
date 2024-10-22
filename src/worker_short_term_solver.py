import os
import ssl
import sys
from celery import Celery
from fastapi.encoders import jsonable_encoder
import sys
from api.tasks.task_manager import AgricoreCelery
from celery.signals import worker_shutting_down, worker_process_shutdown
from celery.worker import state

from loguru import logger
import importlib.metadata
import agricore_sp_models as asp

QUEUE_SUFFIX = os.getenv('QUEUE_SUFFIX')
logger.info("Identified QUEUE_SUFFIX: {0}".format(QUEUE_SUFFIX))
logger.info(f"Agricore models library version: {importlib.metadata.version('agricore_sp_models')}")

# Celery APP configuration
app = Celery()
AgricoreCelery.set_config(app, QUEUE_SUFFIX)
AgricoreCelery.add_sp_solver_tasks(app)

@worker_shutting_down.connect
def worker_shutting_down_handler(sig, how, exitcode, **kwargs):
    # Get the hostname of the current worker
    hostname = kwargs['sender'].split('@')[1]

    print(f'worker_shutting_down({sig}, {how}, {exitcode}) on worker: {hostname}')
    
    # Inspect only the current worker
    i = app.control.inspect([hostname])

    # Revoke active tasks on this worker only
    active_tasks = i.active()
    if active_tasks:
        for task_details in active_tasks[hostname]:
            app.control.revoke(task_details['id'], terminate=True)
            print(f'Task {task_details["id"]} revoked')

    print('worker_shutting_down_handler: tasks revoked on this worker')

# Disabled as this does not seem to be signaled when a forked process is shutting down
# @worker_process_shutdown.connect
# def worker_process_shutdown_handler(*args, **kwargs):
#     # Get the hostname of the current worker
#     hostname = kwargs['sender'].split('@')[1]

#     print(f'worker process shuting down on worker: {hostname}')
    
#     # Inspect only the current worker
#     i = app.control.inspect([hostname])

#     # Revoke active tasks on this worker only
#     active_tasks = i.active()
#     if active_tasks:
#         for task_details in active_tasks[hostname]:
#             app.control.revoke(task_details['id'], terminate=True)
#             print(f'Task {task_details["id"]} revoked')

#     print('worker_shutting_down_handler: tasks revoked on this worker')



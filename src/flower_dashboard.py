import os
import ssl
import sys
from celery import Celery
from fastapi.encoders import jsonable_encoder
import sys
from api.tasks.task_manager import AgricoreCelery
from loguru import logger

# Celery APP configuration
QUEUE_SUFFIX = os.getenv('QUEUE_SUFFIX')

app = Celery()
AgricoreCelery.set_config(app)
AgricoreCelery.add_controller_tasks(app)
AgricoreCelery.add_lp_tasks(app)
AgricoreCelery.add_sp_scheduler_tasks(app)
AgricoreCelery.add_sp_solver_tasks(app)

app.select_queues()

if QUEUE_SUFFIX is not None:
    logger.info(f'Selecting queues with suffix {QUEUE_SUFFIX}')
    app.select_queues([f'simulation_manager_{QUEUE_SUFFIX}', f'long_term_{QUEUE_SUFFIX}', f'short_term_solver_{QUEUE_SUFFIX}', f'short_term_scheduler_{QUEUE_SUFFIX}'])
else:
    logger.info(f'Selecting default queues')
    app.select_queues(['simulation_manager', 'long_term', 'short_term_solver', 'short_term_scheduler'])




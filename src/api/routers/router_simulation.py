from ctypes import Union
from multiprocessing.pool import AsyncResult
import os
import time
from typing import Annotated, Any, Dict, Optional
from celery import Celery
from fastapi import APIRouter
from fastapi import Body
from fastapi.responses import JSONResponse
from loguru import logger
from agricore_sp_models.simulation_models import SimulationScenario
from api.tasks.task_manager import AgricoreCelery
from api.tasks.task_aux import add_simulation_scenario

router = APIRouter(
    prefix="/tasks",
    responses={404: {"description": "Not found"}},
)

celery = Celery(log=logger)
AgricoreCelery.set_config(celery)

@router.post("/simulationScenario/", status_code=201)
def run_task(args: SimulationScenario):
    """
    Handle the request to run a simulation scenario.

    ...

    Parameters
    ----------
    args : SimulationScenario
        The parameters required to run the simulation scenario

    Returns
    -------
    JSONResponse
        A JSON response containing the task ID if successful, or an error message if not

    Raises
    ------
    HTTPException
        If there is an error processing the request
    """
    try:
        debug_mode = False
        if args.queueSuffix is not None and args.queueSuffix != "":
            debug_mode = True
        
        if not debug_mode:
            logger.info(f"Received new Simulation request with parameters {str(args.dict())}")
            id = AgricoreCelery.launch_simulation(celery, [args])    
        else:
            logger.info(f"Received new Simulation request (in debug mode with suffix {args.queueSuffix}) with parameters {str(args.dict())}")
            id = AgricoreCelery.launch_simulation_in_debug_mode(celery, args.queueSuffix, [args])
        return JSONResponse({"task_id": id}, status_code=201)
    except:
        logger.exception(
            "error")
        return JSONResponse({"error": "error"})
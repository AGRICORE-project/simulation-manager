from ctypes import Union
from multiprocessing.pool import AsyncResult
import os
import time
from typing import Annotated, Optional
from celery import Celery
from fastapi import APIRouter
from fastapi import Body
from fastapi.responses import JSONResponse
from loguru import logger

from api.tasks.task_manager import AgricoreCelery

router = APIRouter(
    prefix="/tasks",
    responses={404: {"description": "Not found"}},
)

app = Celery()
agri = AgricoreCelery(app)

@router.get("/tasks/{task_id}")
async def get_status(task_id):
    """
    Get the status of a specific task by its ID.

    ...

    Parameters
    ----------
    task_id : str
        The ID of the task whose status is being requested

    Returns
    -------
    JSONResponse
        A JSON response containing the task ID, name, status, and result
    """
    logger.info("requesting status for tastk: {0}".format(task_id))
    task_result = agri.app.AsyncResult(task_id)
    result = {
        "task_id": task_id,
        "task_name": task_result.name,
        "task_status": task_result.status,
        "task_result": task_result.result
    }
    return JSONResponse(result)
    
@router.post("/tasks", status_code=201)
def run_task(name:str):
    """
    Launch a new task with the given name.

    ...

    Parameters
    ----------
    name : str
        The name of the task to be launched

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
        id = agri.launch_task(name)
        return JSONResponse({"task_id": id}, status_code=201)
    except:
        logger.exception(
            "error")
        return JSONResponse({"error": "error"})



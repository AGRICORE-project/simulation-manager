import time
import uvicorn

from celery.result import AsyncResult
from starlette.middleware.cors import CORSMiddleware
from fastapi import Body, FastAPI, Form, Request
from fastapi.responses import JSONResponse
#from api.routers import router_tasks, router_test, router_farms
# from api.routers import router_farms
from api.routers import router_simulation
from loguru import logger
import importlib.metadata
import agricore_sp_models as asp

logger.info(f"Agricore models library version: {importlib.metadata.version('agricore_sp_models')}")

app = FastAPI()
#app.include_router(router_farms.router)
#app.include_router(router_tasks.router)
#app.include_router(router_test.router)
app.include_router(router_simulation.router)
app.add_middleware(CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)

# @app.middleware("http")
# async def add_process_time_header(request, call_next):
#     start_time = time.time()
#     response = await call_next(request)
#     process_time = time.time() - start_time
#     response.headers["X-Process-Time"] = str(f'{process_time:0.4f} sec')
#     return response

if __name__=="__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8004, access_log=False)
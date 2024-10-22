import importlib
import ssl
import sys
import time
import json
import os
from click import launch
import cloudpickle
import gc

from types import TracebackType
from celery import Celery, signature, chain, group
from celery.result import allow_join_result
import httpx
from loguru import logger
from matplotlib.pylab import f
from pyrsistent import immutable
from collections import Counter

from api.models.opt_models import LongTermOptimizationParameters, ShortTermModelParameters
from settings import settings
from api.tasks.task_aux import *
from api.utils.repository_utils import get_random_string
from api.utils.repository_utils import download_and_extract_branch
from agricore_sp_models.simulation_models import *
from agricore_sp_models.logging import configure_orm_logger

# Monkey patch to solve copytree issues:
import errno, shutil
orig_copyxattr = shutil._copyxattr
def patched_copyxattr(src, dst, *, follow_symlinks=True):
	try:
		orig_copyxattr(src, dst, follow_symlinks=follow_symlinks)
	except OSError as ex:
		if ex.errno != errno.EACCES: raise
shutil._copyxattr = patched_copyxattr

class AgricoreCelery():
    """
    A class used to handle Celery tasks for the Agricore application

    ...

    Methods
    -------
    set_config(app, queue_suffix=None)
        Configures the Celery app with the specified settings

    launch_task(app, name, args=None)
        Launches a Celery task with the given name and arguments

    launch_simulation(app, args=Union[SimulationScenario, None]=None)
        Launches a complete simulation task

    launch_simulation_in_debug_mode(app, queue_suffix, args=...

    add_sp_solver_tasks(app)
        Adds the short term solver tasks to the Celery app

    add_controller_tasks(app)
        Adds the controller tasks to the Celery app
    """
    def set_config(app, queue_suffix=None):
        """
        Configures the Celery application with necessary settings including broker URL, 
        result backend, and task routes.

        Parameters:
            app (Celery): The Celery application instance to configure.
            queue_suffix (Union[str, None], optional): The suffix to append to queue names. Defaults to None.
        """
        CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL')
        CELERY_INSTANCE = os.getenv('CELERY_INSTANCE_NAME')
        app.main = CELERY_INSTANCE
        logger.debug("Using Celery. Name: {0}, URL: {1}".format(CELERY_INSTANCE, CELERY_BROKER_URL))
        app.conf.broker_url = f"{CELERY_BROKER_URL}"
        app.conf.result_backend = f"{CELERY_BROKER_URL}"
        app.conf.visibility_timeout = 43200
        app.conf.result_backend_transport_options = {
            'retry_policy': {
                'timeout': 5.0
            },
            'redis_backend_health_check_interval': 10,
            
        }
        
        if queue_suffix == '':
            queue_suffix = None
            
        if queue_suffix is None:
            logger.info(f'Selecting default queues')
        else:
            logger.info(f'Selecting queues with suffix {queue_suffix}')

        # declare queue for each task-worker. worker-queue relation is done in the .sh file
        app.conf.update(
            task_routes = {
                'run_complete_simulation' : {'queue': f'simulation_manager{"_" + queue_suffix if queue_suffix is not None else ""}'},
                'run_long_term_optimization' : {'queue': f'long_term{"_" + queue_suffix if queue_suffix is not None else ""}'},
                'initialize_folder_for_lt' : {'queue': f'long_term{"_" + queue_suffix if queue_suffix is not None else ""}'},
                'run_short_term_calibration' : {'queue': f'short_term_solver{"_" + queue_suffix if queue_suffix is not None else ""}'},
                'run_short_term_simulation' : {'queue': f'short_term_solver{"_" + queue_suffix if queue_suffix is not None else ""}'},
                'schedule_short_term_simulation' : {'queue': f'short_term_scheduler{"_" + queue_suffix if queue_suffix is not None else ""}'}
            }
        )

        app.conf.update(
            result_expires=60,
            task_acks_late=False,
            enable_utc=True,
            track_started=True,
            # timezone='Europe/Madrid',
            # timezone='UTC',
            result_extended=True,
            task_serializer = "pickle",
            result_serializer = "pickle",
            event_serializer = "json",
            accept_content = ["application/json", "application/x-python-serialize"],
            result_accept_content = ["application/json", "application/x-python-serialize"]
        )

    def launch_task(app, name, args= None):
        """
        Launches a task in the Celery application.

        Parameters:
            app (Celery): The Celery application instance to use.
            name (str): The name of the task to launch.
            args (Union[list, None], optional): The arguments to pass to the task. Defaults to None.

        Returns:
            str: The task ID of the launched task.
        """
        task = app.send_task(name, args)
        return task.id
    
    def launch_simulation(app, args: Union[SimulationScenario, None]=None):
        """
        Launches a complete simulation task.

        Parameters:
            app (Celery): The Celery application instance to use.
            args (Union[SimulationScenario, None], optional): The arguments to pass to the simulation task. Defaults to None.

        Returns:
            str: The task ID of the launched simulation task.
        """
        task = app.send_task("run_complete_simulation", queue=f"simulation_manager", args=args)
        return task.id
    
    def launch_simulation_in_debug_mode(app, queue_suffix, args: Union[SimulationScenario, None]=None):
        """
        Launches a complete simulation task in debug mode.

        Parameters:
            app (Celery): The Celery application instance to use.
            queue_suffix (str): The suffix to append to the queue name for debug mode.
            args (Union[SimulationScenario, None], optional): The arguments to pass to the simulation task. Defaults to None.

        Returns:
            str: The task ID of the launched simulation task.
        """
        task = app.send_task("run_complete_simulation", queue=f"simulation_manager_{queue_suffix}", args=args)
        return task.id
    
    def add_lp_tasks(app):
        @app.task(bind=True, name="initialize_folder_for_lt", max_retries=0)
        def initialize_folder_for_lt(self, args: LongTermOptimizationParameters) -> None:
            """
            Configures the working directory for long-term optimization by downloading and 
            extracting the relevant code from a GitHub repository, if the directory does not 
            already exist.

            Args:
                args (LongTermOptimizationParameters): Parameters required for initializing the 
                    long-term optimization folder, including the simulation run ID and the branch 
                    of the repository to download.

            Raises:
                Exception: If there is an error during the folder initialization process.
            """
            configure_orm_logger(settings.ORM_ENDPOINT)
            with logger.contextualize(simulationRunId=args.simulationRunId, logSource="long_term"):
                logger.info(f"Initializing folder for LT execution for simulationRunId {args.simulationRunId}")
                try:
                    dir_suffix = 'lt_model_sr_' + str(args.simulationRunId)
                    prepath = os.getenv('LT_DEPLOY_DIR')
                    working_dir = os.path.join(prepath, dir_suffix)
                    # only do next lines if working_dir does not exist
                    if not os.path.exists(working_dir):
                        GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
                        LT_MODEL_REPOSITORY = os.getenv('LT_MODEL_REPOSITORY')
                        download_and_extract_branch(GITHUB_TOKEN, LT_MODEL_REPOSITORY, args.branch, working_dir, 
                                                    tmp_dir = prepath + "/latest_" + dir_suffix, tmp_download_file=prepath + "/latest_"+ dir_suffix+".tar")
                    else:
                        logger.info(f"Initialisation not needed as the LT folder was already present for simulationRunId {args.simulationRunId}")
                except Exception as e:
                    #fail_task(self, e, 'Exception on Run Short Term Calibration' + repr(e), False)
                    raise e
        
        @app.task(bind=True, name="run_long_term_optimization", max_retries=0)
        def run_long_term_optimization(self, args: LongTermOptimizationParameters) -> List[int]:
            """
            Executes the long-term optimization model using the provided parameters, retrieves the 
            necessary data, runs the optimization, and saves the results.

            Args:
                args (LongTermOptimizationParameters): Parameters required for running the long-term 
                    optimization, including the simulation run ID, population ID, and year ID.

            Returns:
                List[int]: A list of error codes indicating any issues encountered during the 
                    optimization process.

            Raises:
                Exception: If there is an error during the long-term optimization process.
            """
            configure_orm_logger(settings.ORM_ENDPOINT)
            with logger.contextualize(simulationRunId=args.simulationRunId, logSource="long_term"):
                logger.info(f"Received request to run LT model for populationId:{args.populationId} and yearId {args.yearId}")
                errors:List[int]=[] 
                try:
                    logger.info("Requesting data for optimization")
                    result = get_data_for_long_term_optimization(args)
                    if result is None:
                        fail_task(self, AgricoreTaskError(), 'LT optimization not completed, no data received', False)    
                    else:
                        logger.info("Data properly received. Continuing with the long term optimization")
                        dir_suffix = 'lt_model_sr_' + str(args.simulationRunId)
                        prepath = os.getenv('LT_DEPLOY_DIR')
                        working_dir = os.path.join(prepath, dir_suffix)
                        spec = importlib.util.spec_from_file_location("lt." + dir_suffix, working_dir + "/model/algorithm_ABM.py") # type: ignore
                        foo = importlib.util.module_from_spec(spec) # type: ignore
                        sys.modules["lt." + dir_suffix] = foo # type: ignore
                        spec.loader.exec_module(foo) # type: ignore
                        sys.modules["lt." + dir_suffix] = foo # type: ignore
                        setattr(foo, "__module__", "lt." + dir_suffix)
                        cloudpickle.register_pickle_by_value(foo)
                        output_data = foo.process_inputs(result, args.yearNumber-1, args.simulationRunId, True, 1000) # type: ignore
                        for x in output_data.agroManagementDecisions:
                            x.yearId = args.yearId
                        # save results to DB here
                        result = save_data_from_long_term_optimization(output_data, args.compress)
                        errors = output_data.errorList
                        if result == False:
                            fail_task(self, AgricoreTaskError(), 'LT optimization data could not saved', False)
                        return errors
                        
                except Exception as e:
                    raise e
                return errors
        
    def add_sp_scheduler_tasks(app):
        @app.task(bind=True, name="schedule_short_term_simulation", max_retries=0)
        def schedule_short_term_simulation(self, args: ShortTermModelParameters):
            """
            Schedules and manages the short-term simulation tasks by partitioning the data, 
            launching simulation tasks for each partition, and monitoring their completion status.

            Args:
                args (ShortTermModelParameters): Parameters required for scheduling the 
                    short-term simulation, including the simulation run ID, population ID, 
                    year, and queue suffix.

            Raises:
                Exception: If there is an error during the short-term simulation scheduling 
                    or execution process.
            """
            configure_orm_logger(settings.ORM_ENDPOINT)
            with logger.contextualize(simulationRunId=args.simulationRunId, logSource="short_term_scheduler"):
                logger.info(f"Received request to simulate ST model for populationId:{args.populationId} and year {args.year}")
                try:
                    logger.info("Requesting data for simulation")
                    agents = get_data_for_short_term_optimization(args)
                    if agents is None:
                        fail_task(self, AgricoreTaskError(),'Simulation not completed, no data received', False)
                    else:
                        sim_run = SimulationRun (
                            id=args.simulationRunId,
                            overallStatus=OverallStatus.INPROGRESS,
                            currentYear=args.year,
                            currentStage=SimulationStage.SHORTPERIOD,
                            currentSubstage="Preparation of partitions and folders",
                            currentStageProgress=0,
                            currentSubStageProgress=0,
                            simulationScenarioId=args.populationId
                        )
                        update_simulation_run(sim_run)
                        
                        partitions:Dict[int, List[int]] = {}
                        output_data: List[ValueFromSPDTO] = []
                        
                        # Creating job partitions. We split in groups of regions so each partition only contains up to 500 farms
                        # (indeed, we keep adding regions until we reach 500 farms, but the number of farms may be >500 as the 
                        # involved regions will be included completetly in the partition)
                        logger.info(f"The simulation requested covers: {len(agents.values)} farms")
                        if len(agents.values) > 2000:
                            logger.debug("Spliting simulation by region due to high number of farms")
                            regions: List[int] = [x.cod_RAGR3 for x in agents.values]
                            farm_counter=Counter(regions)
                            next_partition = 0
                            next_partition_regions = []
                            next_partition_farms = 0
                            for region, count in farm_counter.items():
                                next_partition_regions.append(region)
                                next_partition_farms = next_partition_farms + count
                                if next_partition_farms > 500:
                                    partitions[next_partition] = next_partition_regions
                                    next_partition = next_partition + 1
                                    next_partition_regions = []
                                    next_partition_farms = 0
                            if len(next_partition_regions) > 0:
                                partitions[next_partition] = next_partition_regions
                        else:
                            partitions[0] = [x.cod_RAGR3 for x in agents.values]
                            
                        launched_tasks = {}
                        
                        logger.info(f"The simulation will be split into {len(partitions)} partitions")
                        
                        logger.debug("Loading data into the required files for simulation")
                        dir_suffix = 'st_model_sr_' + str(args.simulationRunId)
                        prepath = os.getenv('ST_DEPLOY_DIR')
                        working_dir = os.path.join(prepath, dir_suffix)
                        initialize_folder_for_short_term(args.simulationRunId, args.branch)
                        original_simulation_dir = os.path.join(working_dir, "MODEL_CSV_SIMULATION")
                        random_string = get_random_string(6)
                        
                        # download tar and uncompress at /tmp/random_string folder (create it if it does not exist)
                        temp_dir = os.path.join("/tmp", random_string)
                        uncompressed_dir = os.path.join(temp_dir, f"files")
                        temp_dir_for_S3 = os.path.join(temp_dir, f"S3")
                        if not os.path.exists(temp_dir):
                            os.makedirs(temp_dir)
                        if not os.path.exists(uncompressed_dir):
                            os.makedirs(uncompressed_dir)
                        if not os.path.exists(temp_dir_for_S3):
                            os.makedirs(temp_dir_for_S3)
                        temp_calibration_file = os.path.join(temp_dir, f"calibration_{args.simulationRunId}.tar.gz")
                        download_file_s3(f"calibration_{args.simulationRunId}.tar.gz", temp_calibration_file)
                        extract_tar(temp_calibration_file, uncompressed_dir)
                        # iterate through files inside uncompressed_dir and copy them to simulation_dir
                        calibration_uncompressed_dir = os.path.join(uncompressed_dir,f'calibration_{args.simulationRunId}')
                        for file in os.listdir(calibration_uncompressed_dir):
                            full_path = os.path.join(calibration_uncompressed_dir, file)
                            if os.path.isfile(full_path):
                                shutil.copy(full_path, original_simulation_dir)
                        
                        # Copying calibration files to simulation (much faster here than doing it per each partition)
                        # copy_files_from_calibration(uncompressed_dir, original_dir)
                        sim_run.currentSubstage = "Generation of partitions data and scheduling"
                        update_simulation_run(sim_run)
                                                  
                        # Preparing data and launching tasks
                        for part_i, regions in partitions.items():
                            logger.debug(f"Preparing Partition {part_i} including regions {','.join([str(region) for region in regions])}")
                            logger.debug(f"Creating specific simulation directory")
                            simulation_dir = os.path.join(working_dir, f"MODEL_CSV_SIMULATION_{args.year}_{part_i}")
                            shutil.copytree(original_simulation_dir, simulation_dir)
                            this_part_values = [x for x in agents.values if x.cod_RAGR3 in regions]
                            this_part_agents = DataToSPDTO (
                                values=this_part_values,
                                productGroups=agents.productGroups,
                                policies=agents.policies,
                                policyGroupRelations=agents.policyGroupRelations,
                                farmYearSubsidies=agents.farmYearSubsidies,
                            )
                            insert_data_in_sp_excel_files(simulation_dir, this_part_agents, calibration_phase=False)
                            # Create a tar of the folder simulation_dir and upload it to S3
                            temp_simulation_file_S3 = os.path.join(temp_dir_for_S3, f"simulation_{args.simulationRunId}_year_{args.year}_partition_{part_i}_task.tar.gz")
                            create_directory_tar(simulation_dir, temp_simulation_file_S3)
                            upload_file_s3(temp_simulation_file_S3, f"simulation_{args.simulationRunId}_year_{args.year}_partition_{part_i}_task.tar.gz")
                            logger.debug(f"Data included in the required files for simulation - Part {part_i} and saved to S3")
                            del this_part_values
                            del this_part_agents
                        # saving policies in a different object to avoid memory issues
                        saved_policies = [x.copy(deep=True) for x in agents.policies]
                        del agents
                        gc.collect()
                        
                            
                        logger.info("All partitions simulations prepared")
                        shutil.rmtree(temp_dir)
                        logger.debug("All temporary files removed")
                        # Launching tasks
                        sim_run.currentSubstage = "Launching partition simulation"
                        update_simulation_run(sim_run)
                        for part_i, regions in partitions.items():
                            this_args = args.copy()
                            this_args.partitionBeingProcessed = part_i
                            logger.debug(f"Launching task for processing part - Part {part_i}")
                            launched_tasks[part_i] = self.app.send_task('run_short_term_simulation', [this_args], queue=f"short_term_solver{'_' + this_args.queueSuffix if this_args.queueSuffix is not None else ''}")
                            logger.debug(f"Launched - Part {part_i}")
                            time.sleep(0.01)
                        logger.info("All partitions simulations launched")
                        # Checking task status
                        failed: bool = False
                        completed: List[bool] = [False] * len(partitions)
                        completed_count:int = 0
                        sim_run.currentSubstage = "ST Simulation running"
                        sim_run.currentSubStageProgress = 0
                        update_simulation_run(sim_run)
                        errors : List[int] = [0] * len(partitions)
                        
                        counter_for_debug_messages = 0
                        while (not failed and not all(completed)):
                            #logger.debug(f"Not all tasks completed. Vector completed values: {','.join([str(x) for x in completed])} - {completed_count} of {len(partitions)} partitions completed. (all completed = {str(all(completed))})")
                            for part_i in partitions.keys():
                                if not completed[part_i]:
                                    if launched_tasks[part_i].ready():
                                        if launched_tasks[part_i].failed():
                                            errors[part_i] = 1
                                            logger.debug(f"Partition {part_i} failed. Trying later after all partitions are processed")
                                        completed[part_i] = True
                                        completed_count = completed_count + 1
                                        sim_run.currentSubStageProgress = (int)(completed_count * 100 / len(partitions))
                                        update_simulation_run(sim_run)
                                        logger.info(f"Simulation progressing. Completed {completed_count} of {len(partitions)} partitions for year {args.year} with {sum(errors)} errors")
                                    else:
                                        time.sleep(1)
                            
                            if counter_for_debug_messages % 12 == 0:
                                logger.debug(f"Completed {completed_count} of {len(partitions)} partitions with {sum(errors)} errors")
                            pending_count = len(partitions) - completed_count
                            if pending_count == 0:
                                pass
                            elif pending_count < 10:
                                pending_indexes = [i for x,i in enumerate(completed) if x == False]
                                pending_tasks = [launched_tasks[x].id for x in pending_indexes]
                                if counter_for_debug_messages % 12 == 0:
                                    logger.debug("Less than 10 partitions remaining. Pending tasks ids: " + ', '.join(pending_tasks))
                                time.sleep(1)
                            else:
                                time.sleep(5)
                            counter_for_debug_messages = counter_for_debug_messages + 1
                        
                        logger.debug (f"All partitions completed ({'some' if failed else 'none'} failed)")
                        
                        # Let's reprocess each failed task one by one, waiting for their completion
                        
                        for part_i in [i for i, x in enumerate(errors) if x > 0]:
                            gc.collect()
                            logger.debug(f"Retrying partition {part_i}")
                            this_args = args.copy()
                            this_args.partitionBeingProcessed = part_i
                            launched_tasks[part_i] = self.app.send_task('run_short_term_simulation', [this_args], queue=f"short_term_solver{'_' + this_args.queueSuffix if this_args.queueSuffix is not None else ''}")
                            counter_for_debug_messages = 0
                            while (not failed):
                                counter_for_debug_messages = counter_for_debug_messages + 1
                                if launched_tasks[part_i].ready():
                                    if launched_tasks[part_i].failed():
                                        logger.debug(f"Partition {part_i} failed again. Failing simulation")
                                        failed = True
                                    else:
                                        errors[part_i] = 0
                                    break
                                else:
                                    if counter_for_debug_messages % 12 == 0:
                                        logger.debug(f"Waiting for retried task {part_i} to finish")
                                    time.sleep(5)
                            if failed:
                                break
                                                    
                        if failed:
                            logger.error(f"Failure detected. Exiting simulation.")
                            fail_task(self, AgricoreTaskError(), 'ST optimization failed', False)
                        elif all(completed):
                            sim_run.currentStageProgress = 95
                            sim_run.currentSubstage = "ST Results Processing"
                            sim_run.currentSubStageProgress = 0
                            update_simulation_run(sim_run)
                            random_string = get_random_string(12)
                            temp_dir = os.path.join("/tmp", random_string)
                            uncompressed_dir = os.path.join(temp_dir, f"files")
                            if not os.path.exists(temp_dir):
                                os.makedirs(temp_dir)
                            if not os.path.exists(uncompressed_dir):
                                os.makedirs(uncompressed_dir)
                            for part_i in partitions.keys():
                                sim_file_name = f"simulation_{args.simulationRunId}_year_{args.year}_partition_{part_i}_results.tar.gz"
                                temp_results_file = os.path.join(temp_dir, sim_file_name)
                                download_file_s3(sim_file_name, temp_results_file)
                                extract_tar(temp_results_file, uncompressed_dir)
                                # iterate through files inside uncompressed_dir and copy them to simulation_dir
                                simulation_dir = os.path.join(uncompressed_dir, f"MODEL_CSV_SIMULATION_{args.year}_{part_i}")
                                part_output_data = process_short_term_simulation_output(simulation_dir, args.scenarioDescriptor, saved_policies)
                                if part_output_data is not None:
                                    output_data.extend(part_output_data)
                                else:
                                    logger.error("Failed to execute the SP simulation")
                                    fail_task(self, AgricoreTaskError(), f"Partial simulation returned no results. Partition {part_i}", False)
                            # Delete temp dir
                            shutil.rmtree(temp_dir)
                            logger.debug("Simulation completed. All parts processed, saving results in the DB")
                            result = save_data_from_short_term_optimization(output_data, args.year, simulationRunId = args.simulationRunId, compress = args.compress)
                            if result:
                                logger.debug("Data from SP simulation saved.")
                                logger.info("SP Simulation completed and data properly stored.")
                                sim_run.currentStageProgress = 100
                                sim_run.currentSubStageProgress = 100
                                update_simulation_run(sim_run)
                                del output_data
                                return
                            else:
                                logger.error("Failed to save data from short term")
                        else:
                            logger.error("This point should not be reached. Simulation strangely failed")
                        fail_task(self, AgricoreTaskError(), 'ST optimization data could not saved', False)
                except Exception as e:
                    raise e
                
    def add_sp_solver_tasks(app):
        @app.task(bind=True, name="run_short_term_simulation", max_retries=0, task_reject_on_worker_lost= True)
        def run_short_term_simulation(self, args: ShortTermModelParameters):
            """
            Executes the short-term model simulation for the specified partition and simulation run. 
            It runs the simulation using the data prepared for the specified partition.

            Args:
                args (ShortTermModelParameters): Parameters required for running the short-term 
                    simulation, including the simulation run ID, population ID, year, partition 
                    being processed, and debug flag.

            Raises:
                Exception: If there is an error during the short-term simulation execution process.
            """
            configure_orm_logger(settings.ORM_ENDPOINT)
            with logger.contextualize(simulationRunId=args.simulationRunId, logSource="short_term_solver"):
                logger.debug(f"Received request to simulate ST model for populationId:{args.populationId} and year {args.year}. Partition {args.partitionBeingProcessed}. Task {self.request.id}")
                try:
                    dir_suffix = 'st_model_sr_' + str(args.simulationRunId)
                    prepath = os.getenv('ST_DEPLOY_DIR')
                    working_dir = os.path.join(prepath, dir_suffix)
                    source_file = f"simulation_{args.simulationRunId}_year_{args.year}_partition_{args.partitionBeingProcessed}_task.tar.gz"
                    results_file = f"simulation_{args.simulationRunId}_year_{args.year}_partition_{args.partitionBeingProcessed}_results.tar.gz"
                    simulation_dir = os.path.join(working_dir, f"MODEL_CSV_SIMULATION_{args.year}_{args.partitionBeingProcessed}")
                    # download s3 file (source_file) and uncompress it at simulation_dir
                    temp_dir = os.path.join("/tmp", get_random_string(6))
                    if not os.path.exists(temp_dir):
                        os.makedirs(temp_dir)
                    temp_file = os.path.join(temp_dir, source_file)
                    download_file_s3(source_file, temp_file)
                    extract_tar(temp_file, simulation_dir)
                    
                    run_simulation(simulation_dir, "s_em", include_debug=args.includeDebug)
                    logger.debug("Simulation completed. Uploading results to S3")
                    # Create a tar of the folder simulation_dir and upload it to S3
                    result_path = os.path.join(temp_dir, results_file)
                    create_directory_tar(simulation_dir, result_path)
                    upload_file_s3(result_path, results_file)
                    shutil.rmtree(temp_dir)
                    logger.debug("Uploading complete. Simulation task completed")
                except Exception as e:
                    raise e
        
        @app.task(bind=True, name="run_short_term_calibration", max_retries=0)
        def run_short_term_calibration(self, args: ShortTermModelParameters) -> None:
            """
            Performs calibration for the short-term model using the provided parameters. It retrieves 
            the necessary data, saves it to the required files, and then runs the calibration process.

            Args:
                args (ShortTermModelParameters): Parameters required for the short-term model 
                    calibration, including the simulation run ID, population ID, year, and debug flag.

            Raises:
                Exception: If there is an error during the short-term calibration process.
            """
            configure_orm_logger(settings.ORM_ENDPOINT)
            with logger.contextualize(simulationRunId=args.simulationRunId, logSource="short_term_solver"):
                logger.info(f"Received request to calibrate ST model for populationId:{args.populationId} and year {args.year}")
                try:
                    logger.debug("Requesting data for SP calibration")
                    agents = get_data_for_short_term_calibration(args)
                    if agents is None:
                        fail_task(self, AgricoreTaskError(),'Calibration not completed, no data received', False)    
                    else:
                        logger.debug("Data for SP calibration obtained")
                        dir_suffix = 'st_model_sr_' + str(args.simulationRunId)
                        prepath = os.getenv('ST_DEPLOY_DIR')
                        working_dir = prepath + "/" + dir_suffix + "/MODEL_CSV_CALIBRATION"
                        initialize_folder_for_short_term(args.simulationRunId, args.branch)
                        logger.debug("Saving data for SP calibration into the required files")
                        insert_data_in_sp_excel_files(working_dir, agents)
                        logger.debug("Data for SP calibration properly saved into the required files")
                        logger.debug("Running SP calibration")
                        run_calibration(working_dir, args.includeDebug)
                        logger.debug("SP calibration completed")
                        temp_dir_for_S3 = os.path.join(prepath, f"tempS3")
                        # check if temp_dir_for_S3 exists and create it if not
                        if not os.path.exists(temp_dir_for_S3):
                            os.makedirs(temp_dir_for_S3)
                        temp_dir_for_S3_calibration = os.path.join(temp_dir_for_S3, f"calibration_{args.simulationRunId}")
                        temp_calibration_file_S3 = os.path.join(temp_dir_for_S3, f"calibration_{args.simulationRunId}.tar.gz")
                        copy_files_from_calibration(working_dir, temp_dir_for_S3_calibration)
                        create_directory_tar(temp_dir_for_S3_calibration, temp_calibration_file_S3)
                        upload_file_s3(temp_calibration_file_S3, f"calibration_{args.simulationRunId}.tar.gz")
                        shutil.rmtree(temp_dir_for_S3)
                        logger.debug("Calibration files uploaded to S3")
                except Exception as e:
                    raise e

    def add_controller_tasks(app):
        @app.task(bind=True, name="run_complete_simulation", max_retries=0)
        def run_complete_simulation(self, args: SimulationScenario):
            """
            Executes the complete simulation process including both long-term and short-term simulations. 
            It handles new simulation runs and can also continue from a specified stage if needed.

            Args:
                args (SimulationScenario): The parameters required to run the complete simulation, 
                    including population ID, year ID, model branches, horizon, and stage to continue from.

            Raises:
                Exception: If there is an error during the simulation process, it logs the error and 
                    updates the simulation run status accordingly.
            """
            simulation_run = None
            if args.queueSuffix == '':
                args.queueSuffix = None

            try:
                logger.debug(f"Received complete simulation request with args {args}")

                # is it a re-run?
                continue_from_stage = args.stageToContinueFrom is not None

                sp_year_number = get_year_number_from_id(args.populationId, args.yearId)
                if sp_year_number is None:
                    raise Exception(f"Year with id {args.yearId} does not exist in population {args.populationId}")
                current_year_number = sp_year_number+1

                excludedFarmIds: List[int] = []

                # new simulation scenario
                if not continue_from_stage:
                    logger.debug(f"Adding a new Simulation Run object")

                    # Create simulation run in first year:
                    simulation_run: SimulationRun = SimulationRun(
                        id=-1,
                        currentStage=SimulationStage.DATAPREPARATION,
                        currentYear=current_year_number,
                        overallStatus=OverallStatus.INPROGRESS,
                        currentSubstage="Task initialization",
                        currentStageProgress=0,
                        currentSubStageProgress=0,
                        simulationScenarioId=args.id
                    )
                    sim_run_id = add_simulation_run(simulation_run)
                    simulation_run.id = sim_run_id

                    current_year_id = get_year_id_from_number(args.populationId, current_year_number)
                    if current_year_id is None:
                        current_year_id = add_year(Year(
                            yearNumber=current_year_number,
                            populationId=args.populationId
                        ))
                    if current_year_id is None:
                        raise Exception(
                            f"Error while creating year {current_year_number} during Simulation Run from Simulation Scenario {args.id}")
                    lt_config = LongTermOptimizationParameters(
                        branch=args.longTermModelBranch,
                        populationId=args.populationId,
                        compress= args.compress,
                        yearId=current_year_id,
                        simulationRunId=simulation_run.id,
                        ignoreLP=args.ignoreLP,
                        ignoreLMM=args.ignoreLMM,
                        yearNumber=current_year_number
                    )

                    logger.debug(f"Preparing long term configuration: {lt_config.dict()}")
                    logger.debug("Initializing folder for LT execution")
                    
                    t_initialize_folder_for_lt_s_task = self.app.send_task('initialize_folder_for_lt', [lt_config], queue=f"long_term{'_' + args.queueSuffix if args.queueSuffix is not None else ''}")
                    
                    # Wait for the task to finish
                    while not t_initialize_folder_for_lt_s_task.ready():
                        print("Waiting for initialize_folder_for_lt to finish")
                        time.sleep(1)                   

                    st_config = ShortTermModelParameters(
                        branch=args.shortTermModelBranch,
                        populationId=args.populationId,
                        yearId=current_year_id,
                        compress= args.compress,
                        year=current_year_number,
                        simulationRunId=simulation_run.id,
                        excludedFarmsIds=excludedFarmIds,
                        numberOfYears=args.horizon,
                        initialYear=sp_year_number + 1,
                        includeDebug=False
                    )

                # existing simulation scenario
                else:
                    simulation_run = get_simulation_run_from_simulation_scenario(args.id)
                    if simulation_run is None:
                        raise Exception(f"Error while retrieving Simulation Run from Simulation Scenario {args.id}")
                    current_year_number = sp_year_number + args.yearToContinueFrom
                    if args.yearToContinueFrom == 0:
                        current_year_number = current_year_number+1

                configure_orm_logger(settings.ORM_ENDPOINT)
                with logger.contextualize(simulationRunId=simulation_run.id, logSource="simulation"):
                    for year_in_horizon in range(0 if args.yearToContinueFrom is None else args.yearToContinueFrom,
                                                 args.horizon):

                        if (year_in_horizon > 0):
                            current_year_number = current_year_number + 1
                        if (continue_from_stage and args.stageToContinueFrom == SimulationStage.LONGPERIOD) or not continue_from_stage:
                            # New years should not exists so we need to create them

                            # check if year was already created
                            current_year_id = get_year_id_from_number(args.populationId, current_year_number)
                            if current_year_id is None:
                                current_year_id = add_year(Year(
                                    yearNumber=current_year_number,
                                    populationId=args.populationId
                                ))
                            if current_year_id is None:
                                raise Exception(f"Error while creating year {current_year_number} during Simulation Run from Simulation Scenario {args.id}")

                            simulation_run.overallStatus = OverallStatus.INPROGRESS
                            simulation_run.currentYear = current_year_number
                            simulation_run.currentStage = SimulationStage.LONGPERIOD
                            simulation_run.currentSubstage = "Task initialization"
                            simulation_run.currentStageProgress = 0
                            simulation_run.currentSubStageProgress = 0
                            update_simulation_run(simulation_run)

                            lt_config = LongTermOptimizationParameters(
                                branch=args.longTermModelBranch,
                                populationId=args.populationId,
                                yearId=current_year_id,
                                simulationRunId=simulation_run.id,
                                ignoreLP=args.ignoreLP,
                                ignoreLMM=args.ignoreLMM,
                                compress=args.compress,
                                yearNumber=current_year_number
                            )
                            
                            logger.debug(f"Preparing long term configuration: {lt_config.dict()}")
                            
                            t_initialize_folder_for_lt_s_task = self.app.send_task('initialize_folder_for_lt', [lt_config], queue=f"long_term{'_' + args.queueSuffix if args.queueSuffix is not None else ''}")
                    
                            # Wait for the task to finish
                            while not (t_initialize_folder_for_lt_s_task.ready() or t_initialize_folder_for_lt_s_task.failed()):
                                print("Waiting for initialize_folder_for_lt to finish")
                                time.sleep(1)    
                                
                            if t_initialize_folder_for_lt_s_task.failed():
                                manage_failed_task(t_initialize_folder_for_lt_s_task)

                            t_long_term_opt_s_task = self.app.send_task('run_long_term_optimization', [lt_config], queue=f"long_term{'_' + args.queueSuffix if args.queueSuffix is not None else ''}")

                            while not (t_long_term_opt_s_task.ready() or t_long_term_opt_s_task.failed()):
                                print("Waiting for run_long_term_optimization to finish")
                                time.sleep(1)
                            
                            if t_long_term_opt_s_task.failed():
                                manage_failed_task(t_long_term_opt_s_task)    
                            
                            with allow_join_result():
                                errors = t_long_term_opt_s_task.get(disable_sync_subtasks=False)
                                logger.debug(f"Received errors from LT: {(errors if errors is not None else 'None')}")

                            if errors is not None and len(errors) > 0:
                                logger.debug(
                                    f"Errors found in long term optimization: {errors}. These farms will not be included in next stages")
                                excludedFarmIds.extend(errors)

                            if continue_from_stage:
                                continue_from_stage = False

                        if (continue_from_stage and args.stageToContinueFrom == SimulationStage.SHORTPERIOD) or not continue_from_stage:
                            simulation_run.currentStage = SimulationStage.SHORTPERIOD
                            simulation_run.currentSubstage = "DIRECTORY PREPARATION"
                            update_simulation_run(simulation_run)

                            if continue_from_stage:
                                current_year_id = get_year_id_from_number(args.populationId, current_year_number)

                            st_config = ShortTermModelParameters(
                                branch=args.shortTermModelBranch,
                                populationId=args.populationId,
                                yearId=current_year_id,
                                year=current_year_number,
                                simulationRunId=simulation_run.id,
                                excludedFarmsIds=excludedFarmIds,
                                numberOfYears=args.horizon,
                                initialYear=sp_year_number + 1,
                                includeDebug=False,
                                compress = args.compress,
                                queueSuffix=args.queueSuffix,
                                partitionBeingProcessed=None,
                                scenarioDescriptor="s_em"
                            )

                            if (year_in_horizon == 0 and not (continue_from_stage and args.stageToContinueFrom == SimulationStage.SHORTPERIOD)):
                                simulation_run.currentSubstage = "CALIBRATION"
                                update_simulation_run(simulation_run)

                                t_short_term_calibration_s_task = self.app.send_task('run_short_term_calibration', [st_config], queue=f"short_term_solver{'_' + args.queueSuffix if args.queueSuffix is not None else ''}")
                                while not (t_short_term_calibration_s_task.ready() or t_short_term_calibration_s_task.failed()):
                                    print("Waiting for run_short_term_calibration to finish")
                                    time.sleep(1)
                                    
                                if t_short_term_calibration_s_task.failed():
                                    manage_failed_task(t_short_term_calibration_s_task)

                            simulation_run.overallStatus = OverallStatus.INPROGRESS
                            simulation_run.currentSubstage = "SIMULATION"
                            simulation_run.currentStageProgress = 30
                            update_simulation_run(simulation_run)
                            
                            t_short_term_schedule_s_task = self.app.send_task('schedule_short_term_simulation', [st_config], queue=f"short_term_scheduler{'_' + args.queueSuffix if args.queueSuffix is not None else ''}")
                            while not (t_short_term_schedule_s_task.ready() or t_short_term_schedule_s_task.failed()):
                                print("Waiting for schedule_short_term_simulation to finish")
                                time.sleep(1)
                                
                            if t_short_term_schedule_s_task.failed():
                                manage_failed_task(t_short_term_schedule_s_task)

                            if continue_from_stage:
                                continue_from_stage = False

                    # When simulation is completed, update state
                    simulation_run.overallStatus = OverallStatus.COMPLETED
                    simulation_run.currentSubstage = "COMPLETED"
                    simulation_run.currentSubStageProgress = 100
                    simulation_run.currentStageProgress = 100
                    update_simulation_run(simulation_run)
            except Exception as e:
                simulation_run.overallStatus = OverallStatus.ERROR
                update_simulation_run(simulation_run)
                fail_task(self, e, 'Exception on Run Complete Simulation' + repr(e), False)

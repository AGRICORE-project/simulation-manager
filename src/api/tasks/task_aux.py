import csv
import boto3
import tarfile
from botocore.exceptions import ClientError
import random
from genericpath import isfile
import glob
from io import TextIOWrapper
import os
import shutil
import subprocess
from typing_extensions import NoReturn
import time
import traceback
from types import TracebackType
from celery import Celery, states
import celery
from celery.exceptions import TaskError, Ignore
import httpx
from api.utils.delayed_log import DelayedLog
from agricore_sp_models.simulation_models import LogLevel
from settings import settings
from api.models.opt_models import LongTermOptimizationParameters, ShortTermModelParameters, Year
from api.utils.repository_utils import download_and_extract_branch
from agricore_sp_models.simulation_models import SimulationScenario, SimulationRun
from agricore_sp_models.common_models import PolicyJsonDTO, LandRentDTO
from agricore_sp_models.agricore_sp_models import ValueToSPDTO, ValueFromSPDTO, AgroManagementDecisionFromLP, DataToSPDTO, DataToLPDTO, CropDataDTO, FarmYearSubsidyDTO
from loguru import logger
from typing import Any, Dict, Dict, List, Tuple, Union
from pydantic import parse_obj_as
import pandas as pd
import math
import platform
import gzip
import io
import base64
import json
from api.tasks.task_aux import *

# pyright: reportOptionalMemberAccess=false


class AgricoreTaskError(TaskError):
    pass

def isLinux() -> bool:
    return platform.system() == "Linux"

def isNotCeroOrNan (value: Any) -> bool:
    if value == 0 or value == "inf" or math.isinf(value) or value == "Nan" or value == "nan" or value == "NaN" or math.isnan(value):
        return False
    else:
        return True

def get_data_for_short_term_calibration(args: ShortTermModelParameters) -> Union[DataToSPDTO, None]:
    """
    Fetches data required for short-term calibration from the ORM endpoint.

    Args:
        args (ShortTermModelParameters): Parameters for the short-term model.

    Returns:
        Union[DataToSPDTO, None]: The data required for calibration, or None if the fetch fails.
    """
    try:
        url = f'{settings.ORM_ENDPOINT}/population/{args.populationId}/farms/get/calibrationdata/shortperiod'
        params = {'year': args.year}
        client = httpx.Client()

        headers = {}
        if args.compress:
            headers = {"Accept-Encoding": "gzip"}
        response = client.get(url, params=params, headers=headers, timeout=None)
        
        if response.status_code == 200:
            
            response_json = response.json()
            try:
                response_data = parse_obj_as(DataToSPDTO, response_json)
            except:
                logger.error("Calibration data received but failed to parse it properly")
                return None
            return response_data
        else:
            logger.error("Failed to get data for short term calibration")
            return None
    except Exception as e:
        logger.error("Failed to get data for short term calibration. {0}".format(repr(e)))
        return None
    
def get_year_number_from_id(populationId: int, yearId: int) -> Union[int, None]:
    """
    Retrieves the year number from the given year ID for a specific population.

    Args:
        populationId (int): ID of the population.
        yearId (int): ID of the year.

    Returns:
        Union[int, None]: The year number, or None if not found.
    """
    try:
        url = f'{settings.ORM_ENDPOINT}/population/{populationId}/years/get'
        client = httpx.Client()
        response = client.get(url, timeout=None)

        if response.status_code == 200:
            response_json = response.json()
            year = [x['yearNumber'] for x in response_json if x['id'] == yearId]
            year = year[0] if len(year)==1 else None
            return year
        else:
            logger.error("Failed to get year number from id")
            return None
    except Exception as e:
        logger.error("Failed to get year number from id. {0}".format(repr(e)))
        return None
    
def get_year_id_from_number(populationId: int, year: int) -> Union[int, None]:
    """
    Retrieves the year ID from the given year number for a specific population.

    Args:
        populationId (int): ID of the population.
        year (int): The year number.

    Returns:
        Union[int, None]: The year ID, or None if not found.
    """
    try:
        url = f'{settings.ORM_ENDPOINT}/population/{populationId}/years/get'
        client = httpx.Client()
        response = client.get(url, timeout=None)

        if response.status_code == 200:
            response_json = response.json()
            yearId = [x['id'] for x in response_json if x['yearNumber'] == year]
            yearId = yearId[0] if len(yearId)==1 else None
            return yearId
        else:
            logger.error("Failed to get year id from number")
            return None
    except Exception as e:
        logger.error("Failed to get year id from number. {0}".format(repr(e)))
        return None
    
def get_data_for_long_term_optimization(args: LongTermOptimizationParameters) -> Union[DataToLPDTO, None]:
    """
    Fetches data required for long-term optimization from the ORM endpoint.

    Args:
        args (LongTermOptimizationParameters): Parameters for the long-term optimization model.

    Returns:
        Union[DataToLPDTO, None]: The data required for long-term optimization, or None if the fetch fails.
    """
    try:
        year=get_year_number_from_id(args.populationId, args.yearId)
        url = f'{settings.ORM_ENDPOINT}/population/{args.populationId}/farms/get/simulationdata/longperiod'
        params = {'year': year, 'ignoreLP': args.ignoreLP, 'ignoreLMM': args.ignoreLMM}
        client = httpx.Client()
        headers = {}
        if args.compress:
            headers = {"Accept-Encoding": "gzip"}
        response = client.get(url, params=params, headers=headers, timeout=None)
        if response.status_code == 200:

            response_json = response.json()

            response_object = parse_obj_as(DataToLPDTO, response_json)
            return response_object
        else:
            logger.error("Failed to get data for long term optimization")
            return None
    except Exception as e:
        logger.error("Failed to get data for long term optimization. {0}".format(repr(e)))
        return None
    
def get_data_for_short_term_optimization(args: ShortTermModelParameters) -> Union[DataToSPDTO, None]:
    """
    Fetches data required for short-term optimization from the ORM endpoint.

    Args:
        args (ShortTermModelParameters): Parameters for the short-term optimization model.

    Returns:
        Union[DataToSPDTO, None]: The data required for short-term optimization, or None if the fetch fails.
    """
    try:
        url = f'{settings.ORM_ENDPOINT}/population/{args.populationId}/farms/get/simulationdata/shortperiod'
        params = {'year': args.year}
        client = httpx.Client()
        headers = {}

        if args.compress:
            headers = {"Accept-Encoding": "gzip"}
        response = client.get(url, params=params, headers=headers, timeout=None)
        
        if response.status_code == 200:
            response_json = response.json()

            response_data = parse_obj_as(DataToSPDTO, response_json)
            return response_data
        else:
            logger.error("Failed to get data for short term optimization")
            return None
    except Exception as e:
        logger.error("Failed to get data for short term optimization. {0}".format(repr(e)))
        return None
    
def save_data_from_long_term_optimization(dataToSave: AgroManagementDecisionFromLP, compress: bool = False) -> bool:
    """
    Saves the results of long-term optimization to the ORM endpoint.

    Args:
        dataToSave (AgroManagementDecisionFromLP): The data to save.
        compress (bool): Whether to compress the data before sending.

    Returns:
        bool: True if the save was successful, False otherwise.
    """
    try:
        url = f'{settings.ORM_ENDPOINT}/results/longperiod'
        client = httpx.Client()
        headers = {"Content-Type": "application/json"} 
        if compress:
            data_to_send = gzip.compress(json.dumps(dataToSave.dict()).encode('utf-8'))
            headers["Content-Encoding"] = "gzip"
        else:
            data_to_send = dataToSave.json()
        response = client.post(url, data=data_to_send, headers=headers, timeout=None)
        if response.status_code == 201:
            return True
        else:
            logger.exception("Failed to save data from LP to database")
            logger.error("Response content: {0}".format(response.content))
            logger.error("Response status code: {0}".format( response.status_code))
            return False
    except Exception as e:
        logger.error("Failed to save data from LP to database. {0}".format(repr(e)))
        return False

def save_data_from_short_term_optimization(dataToSave: List[ValueFromSPDTO], year: int, simulationRunId: int = 0, compress: bool = False) -> bool:
    """
    Saves the results of short-term optimization to the ORM endpoint.

    Args:
        dataToSave (List[ValueFromSPDTO]): The data to save.
        year (int): The year of the simulation.
        simulationRunId (int): The ID of the simulation run.
        compress (bool): Whether to compress the data before sending.

    Returns:
        bool: True if the save was successful, False otherwise.
    """
    try:
        url = f'{settings.ORM_ENDPOINT}/results/shortperiod/simulation?year={year}&simulationRunId={simulationRunId}'
        client = httpx.Client()
        headers = {"Content-Type": "application/json"}

        data_to_send = json.dumps([x.dict() for x in dataToSave])
        if compress:
            data_to_send = gzip.compress(data_to_send.encode('utf-8'))
            headers["Content-Encoding"] = "gzip"
        
        response = client.put(url, data=data_to_send, headers=headers, timeout=None)
        if response.status_code == 201:
            return True
        else:
            logger.error("Failed to save data from SP to database")
            return False
    except Exception as e:
        logger.error("Failed to save data from SP to database. {0}".format(repr(e)))
        return False


def add_simulation_scenario(args: SimulationScenario) -> Union[int, None]:
    """
    Adds a new simulation scenario to the database.

    Args:
        args (SimulationScenario): The simulation scenario to add.

    Returns:
        Union[int, None]: The ID of the added simulation scenario, or None if the operation fails.
    """
    try: 
        url = f'{settings.ORM_ENDPOINT}/population/{args.populationId}/year/{args.yearId}/simulationScenario/add'
        client = httpx.Client()
        response = client.post(url, json=args.dict(exclude={'id'}), timeout=None)
        if response.status_code == 201:
            id = response.json()['id']
            logger.info("Simulation scenario with id {0} saved".format(id))
            return id
        else:
            logger.error("failed to save Simulation Scenario to database")
            return None
    except Exception as e:
        logger.error("failed to save Simulation Scenario to database. {0}".format(repr(e)))
        return None

def get_simulation_run_from_simulation_scenario(simulationScenarioId: int) -> Union[SimulationRun,None]:
    """
    Retrieves the simulation run associated with a given simulation scenario ID.

    Args:
        simulationScenarioId (int): The ID of the simulation scenario.

    Returns:
        Union[SimulationRun, None]: The associated simulation run, or None if the operation fails.
    """
    try:
        url = f'{settings.ORM_ENDPOINT}/simulationScenario/{simulationScenarioId}/simulationRun/get'
        client = httpx.Client()
        response = client.get(url, timeout=None)
        if response.status_code == 200:
            response_json = response.json()
            simulation_run: SimulationRun = parse_obj_as(SimulationRun, response_json)
            #logger.debug("Simulation run with id {0} retrieved".format(simulation_run.id))
            return simulation_run
        else:
            logger.error("failed to retrieve Simulation Run from database")
            return None
    except Exception as e:
        logger.error("failed to retrieve Simulation Run from database. {0}".format(repr(e)))
        return None

def add_simulation_run(args: SimulationRun) -> Union[int, None]:
    """
    Adds a new simulation run to the database.

    Args:
        args (SimulationRun): The simulation run to add.

    Returns:
        Union[int, None]: The ID of the added simulation run, or None if the operation fails.
    """
    try:
        url = f'{settings.ORM_ENDPOINT}/simulationScenario/{args.simulationScenarioId}/simulationRun/add'
        client = httpx.Client()
        response = client.post(url, json=args.dict(exclude={'id'}), timeout=None)
        if response.status_code == 201:
            id = response.json()['id']
            #logger.debug("Simulation run with id {0} saved".format(id))
            return id
        else:
            logger.error("failed to add Simulation Run to database")
            return None
    except Exception as e:
        logger.error("failed to add Simulation Run to database. {0}".format(repr(e)))
        return None
        
def update_simulation_run(args: SimulationRun) -> bool:
    """
    Updates an existing simulation run in the database.

    Args:
        args (SimulationRun): The simulation run to update.

    Returns:
        bool: True if the update was successful, False otherwise.
    """
    try:
        url = f'{settings.ORM_ENDPOINT}/simulationRun/{args.id}/edit'
        client = httpx.Client()
        response = client.put(url, json=args.dict(), timeout=None)
        if response.status_code == 200:
            #logger.debug("Simulation run with id {0} updated".format(args.id))
            return True
        else:
            logger.error("failed to update Simulation Run to database")
            return False
    except Exception as e:
        logger.error("failed to update Simulation Run to database. {0}".format(repr(e)))
        return False
    
def add_year(args: Year) -> Union[int, None]:
    """
    Adds a new year to the database.

    Args:
        args (Year): The year to add.

    Returns:
        Union[int, None]: The ID of the added year, or None if the operation fails.
    """
    try:
        url = f'{settings.ORM_ENDPOINT}/population/{args.populationId}/years/add'
        client = httpx.Client()
        response = client.post(url, json=args.dict(exclude={'id'}), timeout=None)
        if response.status_code == 201:
            id = response.json()['id']
            logger.debug("Year with id {0} saved".format(id))
            return id
        else:
            logger.error("failed to add Year to database")
            return None
    except Exception as e:
        logger.error("failed to add Year Run to database. {0}".format(repr(e)))
        return None

def fix_config_files(folder_path:str, farms_data: DataToSPDTO) -> None:
    """
    Cleans up and fixes configuration CSV files in a given folder based on the provided farm data.

    Args:
        folder_path (str): The path to the folder containing the configuration files.
        farms_data (DataToSPDTO): The farm data used to determine which lines to keep in the configuration files.

    Returns:
        None
    """
    # cleans up csvs from calibration or simulation folders
    csv_file_emission = os.path.join(folder_path, "emission.csv")
    csv_file_waterfootprint = os.path.join(folder_path, "waterfootprint.csv")
    set_emi_file = os.path.join(folder_path, "SET_emi.txt")

    procs = []
    for cropName in farms_data.values[0].crops:
        procs.append(cropName if cropName != "DAIRY" else "MILK")
    procs = list(set(procs))
    procs.sort()
    
    keep_lines = []
    with open(csv_file_emission) as file:
        lines = [line.rstrip() for line in file]
        for line in lines:
            fields = line.split(',')
            crop = fields[0].replace('\'','')
            if crop in procs:
                keep_lines.append(line)
    with open(csv_file_emission, 'w') as file:
        for line in keep_lines:
            file.write(line + "\n")

    keep_lines = []
    with open(csv_file_waterfootprint) as file:
        lines = [line.rstrip() for line in file]
        first_line = lines.pop(0)
        keep_lines.append(first_line)
        for line in lines:
            fields = line.split(',')
            crop = fields[0].replace('\'','')
            if crop in procs:
                keep_lines.append(line)
    with open(csv_file_waterfootprint, 'w') as file:
        for line in keep_lines:
            file.write(line + "\n")
            
    for fileName in [set_emi_file]:
        keep_lines = []
        with open(fileName) as file:
            lines = [line.rstrip() for line in file]
            for line in lines:
                if line in procs:
                    keep_lines.append(line)
        with open(fileName, 'w') as file:
            for line in keep_lines:
                file.write(line + "\n")

# Calibration_phase activates the generation of the whole_population files. In the same way, 
# the environment_file is only created whten it is not calibration_stage, as it includes the performance
# ratio compared to the une used in the calibration.
    
def insert_data_in_sp_excel_files(folder_path:str, farms_data: DataToSPDTO, calibration_phase:bool = True) -> None:
    """
    Insert data from the farms_data object into various CSV and TXT files in the specified folder_path.

    Args:
    - folder_path (str): The directory path where the CSV and TXT files will be written.
    - farms_data (DataToSPDTO): An object containing data to be inserted into CSV and TXT files.
    - calibration_phase (bool, optional): Flag indicating whether it's a calibration phase. Defaults to True.

    Raises:
    - AgricoreTaskError: If there is any error during the data insertion process.

    """
    try:
        csv_file_AGENT_BASED = os.path.join(folder_path, "AGENT_BASED.csv")
        csv_file_COD_FARM = os.path.join(folder_path, "COD_FARM.csv")
        csv_file_C_VAR = os.path.join(folder_path, "C_VAR.csv")
        csv_file_COD_RAGR = os.path.join(folder_path, "COD_RAGR.csv")
        csv_file_PR = os.path.join(folder_path, "PR.csv")
        csv_file_PROD = os.path.join(folder_path, "PROD.csv")
        csv_file_SUBSIDIES = os.path.join(folder_path, "SUBSIDIES.csv")
        csv_file_COUP_SUBSIDIES = os.path.join(folder_path, "COUPLED_SUBS.csv")
        csv_file_COUP_SUBSIDIES_v2_payments = os.path.join(folder_path, "COUPLED_SUBS_v2_payments.csv")
        csv_file_COUP_SUBSIDIES_v2_rates = os.path.join(folder_path, "COUPLED_SUBS_v2_rates.csv")
        csv_file_UAA = os.path.join(folder_path, "UAA.csv")
        csv_file_stable = os.path.join(folder_path, "stable.csv")
        csv_rental_price_file = os.path.join(folder_path, "rental_price.csv")
        csv_rents_file = os.path.join(folder_path, "rents.csv")
        csv_greening_file = os.path.join(folder_path, "greening_surface.csv")
        csv_tax_emission = os.path.join(folder_path, "tax_emission.csv")
        csv_environment_file = os.path.join(folder_path, "environment.csv")
        txt_file_procs = os.path.join(folder_path, "SET_proc.txt")
        txt_file_whole_population_procs = os.path.join(folder_path, "SET_whole_population_proc.txt")
        txt_file_regions = os.path.join(folder_path, "SET_agrarian_region.txt")
        txt_file_farms = os.path.join(folder_path, "SET_farm.txt")
        txt_file_whole_population_farms = os.path.join(folder_path, "SET_whole_population_farm.txt")
        txt_file_coupled_subsidies = os.path.join(folder_path, "coupled_subsidies.txt")
        txt_file_coupled_subsidies_v2 = os.path.join(folder_path, "coupled_subsidies_v2_names.txt")
        txt_file_tax_level = os.path.join(folder_path, "SET_tax_level.txt")
        
        
        # Lets get the list of procs
        procs=[]
        for cropName in farms_data.values[0].crops:
            procs.append(cropName if cropName != "DAIRY" else "MILK")
        procs = list(set(procs))
        procs.sort()
        
        # Then filter for those that have some production only
        filtered_procs=[]
        for proc in procs:
            produced = False
            for farm in farms_data.values:
                # has production consumed or sold?
                
                if (proc in farm.crops):
                    produces = ((isNotCeroOrNan(farm.crops[proc].quantitySold) and farm.crops[proc].quantitySold > 0) or
                    (isNotCeroOrNan(farm.crops[proc].quantityUsed) and farm.crops[proc].quantityUsed > 0))
                    if (produces):
                        produced = True
                        break
            filtered_procs.append(proc)
        procs = filtered_procs
        
        
        # Ensure that MILK is always inserted in the output, as it is required by the model
        if "MILK" not in procs:
            procs.append("MILK")

        # Create one file for each category
        # Let's extract the categories:
        categories = []
        for pg in farms_data.productGroups:
            name = pg.name if pg.name!= "DAIRY" else "MILK"
            if name in procs:
                for cat in pg.modelSpecificCategories:
                    categories.append(cat)
        categories = list(set(categories))
        
        # required_categories = ["Cereal", "COPOrIndustrial", "Semipermanent", "MeadowsAndPastures", "FixingNitrogen"]
        
        for category in categories:
            txt_category_file = os.path.join(folder_path, f'SET_{category}_procs.txt')
            with open(txt_category_file, 'w') as file:
                for pg in farms_data.productGroups:
                    name = pg.name if pg.name!= "DAIRY" else "MILK"
                    if name in procs:
                        if category in pg.modelSpecificCategories:
                            file.write(name + "\n")
                
        # Creating special Vegetal Crops file (arable without milk):
        txt_vegetal_crops_file = os.path.join(folder_path, "SET_vegetal_procs.txt")
        with open(txt_vegetal_crops_file, 'w') as file:
            for pg in farms_data.productGroups:
                name = pg.name if pg.name!= "DAIRY" else "MILK"
                if pg.name in procs and pg.name != "MILK":
                    if "Arable" in pg.modelSpecificCategories:
                        proc_name = pg.name 
                        file.write(proc_name + "\n")
                
        with open(txt_file_coupled_subsidies, 'w') as file:
            pgs_with_subsidy = []
            for pgr in farms_data.policyGroupRelations:
                pgs_with_subsidy.append(pgr.productGroupName)
            pgs_with_subsidy = list(set(pgs_with_subsidy))
            for pg in pgs_with_subsidy:
                file.write(pg + "\n")
        
        # Common for the three new coupled subsidies files:
        policy_group_relations = farms_data.policyGroupRelations
        farmYearSubsidies = farms_data.farmYearSubsidies
        coupled_products = [pgr.productGroupName for pgr in policy_group_relations]
        coupled_products = [value for value in coupled_products if value in procs]
        coupled_products = list(set(coupled_products))
        if (len(coupled_products) == 0):
            coupled_products.append(procs[0])
        pgr_dict = {(pgr.policyIdentifier, pgr.productGroupName):pgr.economicCompensation for pgr in policy_group_relations}
        coupled_policies_list = list(set([pgr.policyIdentifier for pgr in policy_group_relations]))
        
        with open(txt_file_coupled_subsidies_v2, 'w') as file:
            if len(coupled_policies_list) > 0:
                for policy in coupled_policies_list:
                    file.write(policy + "\n")
            else:
                # We need the file to have content in order for GAMS not to fail
                file.write("DUMMY" + "\n")
                
        with open(csv_file_COUP_SUBSIDIES_v2_rates, 'w') as file:
            writer = csv.writer(file)
            headers = ['policyIdentifier']
            headers.extend(coupled_products)    
            writer.writerow(headers)
                       
            if (len(coupled_policies_list) > 0):                     
                for policy in coupled_policies_list:
                    values = [policy]
                    for product in coupled_products:
                        values.append(str(pgr_dict.get((policy,product),0)))
                    writer.writerow(values)
            else:
                # We need the file to have content in order for GAMS not to fail
                values = ["DUMMY",0]
                writer.writerow(values)
                
        with open(csv_file_COUP_SUBSIDIES_v2_payments, 'w', newline='') as file:
            writer = csv.writer(file)
            policies = farms_data.policies
            farmYearSubsidies = farms_data.farmYearSubsidies
            
            headers: List[str] = ['farm']
            headers.extend(coupled_policies_list)
            if len(coupled_policies_list) == 0:
                headers.append("DUMMY")
            writer.writerow(headers)
            
            if (len(coupled_policies_list) > 0):
                subsidies_dict = {(x.farmId,x.policyIdentifier):x.value for x in farmYearSubsidies if x.policyIdentifier in coupled_policies_list}
                for farm in farms_data.values:
                    values = [str(farm.farmCode)]
                    for policy in coupled_policies_list:
                        values.append(str(subsidies_dict.get((farm.farmCode,policy),0)))
                    writer.writerow(values)
            else:
                for farm in farms_data.values:
                    values = [str(farm.farmCode), 0]
                    writer.writerow(values)
        
        with open(txt_file_procs, 'w') as file:
            for proc in procs:
                file.write(proc + "\n")
        if calibration_phase:
            with open(txt_file_whole_population_procs, 'w') as file:
                for proc in procs:
                    file.write(proc + "\n")

        with open(txt_file_regions, 'w') as file:
            regions = []
            for farm in farms_data.values:
                regions.append(farm.cod_RAGR3)
            regions = list(set(regions))
            regions.sort()
            for region in regions:
                file.write(str(region) + "\n")
        
        
        farm_codes = []
        for farm in farms_data.values:
            farm_codes.append(farm.farmCode)
        farm_codes = list(set(farm_codes))
        farm_codes.sort()
        with open(txt_file_farms, 'w') as file:
            for farm_code in farm_codes:
                file.write(str(farm_code) + "\n")

        if calibration_phase:
            with open(txt_file_whole_population_farms, 'w') as file:
                for farm_code in farm_codes:
                    file.write(str(farm_code) + "\n")
            

        # ---- holderInfo ----> AGENT_BASED.csv
        # Extract values and write to CSV
        with open(csv_file_AGENT_BASED, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['farm', 'cod_RAGR', 'age', 'successors', 'successor_age',
                            'cod_RAGR', 'OTE', 'altitude', 'family_members', 'current_Asset'])

            for farm in farms_data.values:
                writer.writerow([farm.farmCode, farm.cod_RAGR3, farm.holderInfo.holderAge, farm.holderInfo.holderSuccessors, farm.holderInfo.holderSuccessorsAge,
                                farm.cod_RAGR3, farm.technicalEconomicOrientation, farm.altitude.value, farm.holderInfo.holderFamilyMembers, farm.currentAssets])
                
        with open(csv_rents_file, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['origin', 'destination', 'area', 'rent_total_price'])
            for farm in farms_data.values:
                for rent in farm.rentedInLands:
                    writer.writerow([rent.originFarmId, rent.destinationFarmId, rent.rentArea, rent.rentValue])

        # ---- farm codes ----> COD_FARM.csv
        # Extract values and write to CSV, no column headers
        with open(csv_file_COD_FARM, 'w', newline='') as file:
            writer = csv.writer(file)
            for farm in farms_data.values:
                writer.writerow([farm.farmCode, farm.cod_RAGR, farm.altitude.value])
                
        with open(csv_rental_price_file, 'w', newline='') as file:
            writer = csv.writer(file)
            for farm in farms_data.values:
                rent_prices = []
                avg_rent_price = 589
                for rent in farm.rentedInLands:
                    rent_prices.append(rent.rentValue/rent.rentArea)
                if (len(rent_prices) > 0):
                    avg_rent_price = sum(rent_prices)/len(rent_prices)
                writer.writerow([farm.farmCode, avg_rent_price])
                
        with open(csv_greening_file, 'w', newline='') as file:
            writer = csv.writer(file)
            for farm in farms_data.values:
                writer.writerow([farm.farmCode, farm.greeningSurface])

        # ---- Crops-VariableCosts ----> C_VAR.csv
        # Extract values and write to CSV, column headers
        with open(csv_file_C_VAR, 'w', newline='') as file:
            writer = csv.writer(file)
            headers: List[str] = ['farm']
            for cropName in farms_data.values[0].crops:
                name = cropName if cropName!= "DAIRY" else "MILK"
                if name in procs:
                    headers.append(name)
            writer.writerow(headers)
            
            for farm in farms_data.values:
                value_list:List[str] = [str(farm.farmCode)]
                for cropName in farm.crops:
                    name = cropName if cropName!= "DAIRY" else "MILK"
                    if name in procs:
                        value_list.append(str(farm.crops[cropName].cropVariableCosts) if isNotCeroOrNan(farm.crops[cropName].cropVariableCosts) else '0')
                writer.writerow(value_list)

        # ---- farm codes ----> COD_RAGR.csv
        with open(csv_file_COD_RAGR, 'w', newline='') as file:
            writer = csv.writer(file)
            # TODO: Remove temporary fix to region 12
            for farm in farms_data.values:
                #writer.writerow([farm.farmCode, 12, farm.altitude.value])
                writer.writerow([farm.farmCode, farm.cod_RAGR3, farm.altitude.value])

        # ---- selling price ----> PR.csv
        with open(csv_file_PR, 'w', newline='') as file:
            writer = csv.writer(file)
            headers: List[str] = ['farm']
            for cropName in farms_data.values[0].crops:
                name = cropName if cropName!= "DAIRY" else "MILK"
                if name in procs:
                    headers.append(name)
            writer.writerow(headers)
            for farm in farms_data.values:
                value_list:List[str] = [str(farm.farmCode)]
                for cropName in farm.crops:
                    name = cropName if cropName!= "DAIRY" else "MILK"
                    if name in procs:
                        value_list.append(str(farm.crops[cropName].cropSellingPrice) if isNotCeroOrNan(farm.crops[cropName].cropSellingPrice) else '0')
                writer.writerow(value_list)

        # ---- Production ----> PROD.csv
        with open(csv_file_PROD, 'w', newline='') as file:
            writer = csv.writer(file)
            headers: List[str] = ['farm']
            for cropName in farms_data.values[0].crops:
                name = cropName if cropName!= "DAIRY" else "MILK"
                if name in procs:
                    headers.append(name)
            writer.writerow(headers)
            for farm in farms_data.values:
                value_list:List[str] = [str(farm.farmCode)]
                for cropName in farm.crops:
                    name = cropName if cropName!= "DAIRY" else "MILK"
                    if name in procs:
                        produced = 0
                        if isNotCeroOrNan(farm.crops[cropName].quantitySold):
                            produced = produced + farm.crops[cropName].quantitySold
                        if isNotCeroOrNan(farm.crops[cropName].quantityUsed):
                            produced = produced + farm.crops[cropName].quantityUsed
                        value_list.append(str(produced))
                writer.writerow(value_list)
                
        # --- Environment.csv ---
        # ---- Production ----> PROD.csv
        with open(csv_environment_file, 'w', newline='') as file:
            writer = csv.writer(file)
            headers: List[str] = ['farm']
            for cropName in farms_data.values[0].crops:
                name = cropName if cropName!= "DAIRY" else "MILK"
                if name in procs:
                    headers.append(name)
            writer.writerow(headers)
            for farm in farms_data.values:
                value_list:List[str] = [str(farm.farmCode)]
                for cropName in farm.crops:
                    name = cropName if cropName!= "DAIRY" else "MILK"
                    if name in procs:
                        environmental_factor = round(random.uniform(-0.15,0.15))
                        value_list.append(str(environmental_factor))
                writer.writerow(value_list)

        # ---- Subsidies ----> SUBSIDIES.csv
        with open(csv_file_SUBSIDIES, 'w', newline='') as file:
            writer = csv.writer(file)
            #headers: List[str] = ['farm','P_GREEN', 'P_BASIC', 'P_PENSION']
            headers: List[str] = ['farm','P_GREEN', 'P_BASIC']
            writer.writerow(headers)
            
            labels_dict = {x.policyIdentifier:x.modelLabel for x in farms_data.policies}
            
            green_subsidies = {x.farmId:x for x in farms_data.farmYearSubsidies if labels_dict[x.policyIdentifier] == "Greening"}
            basic_subsidies = {x.farmId:x for x in farms_data.farmYearSubsidies if labels_dict[x.policyIdentifier] == "Basic"}
            
            for farm in farms_data.values:
                farmId = farm.farmCode
                value_list:List[str] = [
                    str(farmId), 
                    str(green_subsidies[farmId].value if farmId in green_subsidies else 0), 
                    str(basic_subsidies[farmId].value if farmId in basic_subsidies else 0)]
                    # str(subsidy.decoupledSubsidies.pPension)]
                writer.writerow(value_list)

        with open(csv_file_COUP_SUBSIDIES, 'w', newline='') as file:
            writer = csv.writer(file)
            policies = farms_data.policies
            product_groups = farms_data.productGroups
            policy_group_relations = farms_data.policyGroupRelations
            farmYearSubsidies = farms_data.farmYearSubsidies
            
            coupled_products = [pgr.productGroupName for pgr in policy_group_relations]
            coupled_products = [value for value in coupled_products if value in procs]
            coupled_products = list(set(coupled_products))
            
            policy_to_pg_dict = {pgr.policyIdentifier:pgr.productGroupName for pgr in policy_group_relations}
                
            headers: List[str] = ['farm']
            headers.extend(coupled_products)
            writer.writerow(headers)
            
            subsidies_dict = {(x.farmId,policy_to_pg_dict[x.policyIdentifier]):x.value for x in farmYearSubsidies if x.policyIdentifier in policy_to_pg_dict}
            
            for farm in farms_data.values:
                values = [str(farm.farmCode)]
                for product in coupled_products:
                    values.append(str(subsidies_dict.get((farm.farmCode,product),0)))
                writer.writerow(values)
        
        # ---- UAA ----> UAA.csv
        with open(csv_file_UAA, 'w', newline='') as file:
            writer = csv.writer(file)
            headers: List[str] = ['farm']
            for cropName in farms_data.values[0].crops:
                name = cropName if cropName!= "DAIRY" else "MILK"
                if name in procs:
                    headers.append(name)
            writer.writerow(headers)
            for farm in farms_data.values:
                value_list:List[str] = [str(farm.farmCode)]
                for cropName in farm.crops:
                    name = cropName if cropName!= "DAIRY" else "MILK"
                    if name in procs:
                        value_list.append(str(farm.crops[cropName].cropProductiveArea) if isNotCeroOrNan(farm.crops[cropName].cropProductiveArea) else '0')
                writer.writerow(value_list)
            
        with open(txt_file_tax_level, 'w', newline='') as file:
            file.write('tax1' + "\n")
            
        with open(csv_tax_emission, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["tax1", "25"])
                
        # ---- Livestock ---> stable.csv
        with open(csv_file_stable, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['farm', 'rebreeding_cows', 'dairy_cows',
                            'tot_LSU', 'pr_milk', 'Prod_milk', 'c_milk'])

            for farm in farms_data.values:
                tot_lsu= 0
                if farm.livestock.dairyCows != 0:
                    tot_lsu += farm.livestock.dairyCows if isNotCeroOrNan(farm.livestock.dairyCows) else 0
                if farm.livestock.rebreedingCows != 0:
                    tot_lsu += farm.livestock.rebreedingCows if isNotCeroOrNan(farm.livestock.rebreedingCows) else 0
                writer.writerow([str(farm.farmCode), 
                                str(farm.livestock.rebreedingCows) if isNotCeroOrNan(farm.livestock.rebreedingCows) else '0', 
                                str(farm.livestock.dairyCows) if isNotCeroOrNan(farm.livestock.dairyCows) else '0',
                                str(tot_lsu), 
                                str(farm.livestock.milkSellingPrice) if isNotCeroOrNan(farm.livestock.milkSellingPrice) else '0', 
                                str(farm.livestock.milkProduction) if isNotCeroOrNan(farm.livestock.milkProduction) else '0', 
                                str(farm.livestock.variableCosts) if isNotCeroOrNan(farm.livestock.variableCosts) else '0'])
        
        fix_config_files(folder_path, farms_data = farms_data)
        
        logger.debug(f"Data insertion in csv files has been completed {folder_path}")
    except Exception as e:
        logger.error("Failed to insert data in csv files. {0}".format(repr(e)))
        raise AgricoreTaskError("Failed to insert data in csv files. {0}".format(repr(e)))
    
def copy_files_from_calibration(calibration_path: str, dest_path: str) -> bool:
    """
    Copies necessary calibration files from the given calibration path to the destination path.

    Args:
        calibration_path (str): The source directory where calibration files are located.
        dest_path (str): The destination directory where calibration files should be copied.

    Returns:
        bool: True if the files are successfully copied, False otherwise.
    """
    calibration_results_file = "calibration_results.gdx"
    calibration_file_paths = ['qmat.csv',
                            'fu.csv', 'nsolx.csv', 'nlsolshx.csv', 'ccostq.csv', 'nlobj.csv', 'costqa.csv', 'nsoly.csv', 'ADCC.csv', 'subj1.prn', 'SET_whole_population_farm.txt', 'SET_whole_population_proc.txt']

    result = True
    
    if not os.path.exists(dest_path):
        os.makedirs(dest_path)
    
    # Keep track of how many files exist
    num_files_exist = 0   
    start_time = 0
    # the calibration file should have been generated
    while not os.path.exists(os.path.join(calibration_path,calibration_results_file)):
        start_time+=1
        time.sleep(1)  # Wait for 1 second before checking again
        logger.info("Waiting for calibration to finish")
        if start_time > 10:
            logger.warning("Timeout: Calibration results file not found.")
            return False
    try:
        for excel_file_path in [calibration_results_file] + calibration_file_paths:
            if os.path.exists(os.path.join(calibration_path, excel_file_path)):
                num_files_exist += 1

        # Check if all necessary files exist
        if num_files_exist == (1 + len(calibration_file_paths)):
            logger.debug(f"Copying calibration files to simulation_module {dest_path}")
            # Copy calibration files to the simulation folder
            for calibration_file in calibration_file_paths:
                calibration_file_full_path = os.path.join(calibration_path, calibration_file)
                # if is a file, copy it
                if os.path.isfile(calibration_file_full_path):
                    shutil.copy(calibration_file_full_path, dest_path)
            # copy calibration results to the simulation folder
            shutil.copy(os.path.join(calibration_path, calibration_results_file), dest_path)

            logger.debug(f"Calibration files copied into simulation. ({dest_path})")
        else:
            logger.error(f"Some problem at server-worker side while copying files to simulation ({dest_path})")
            return False
    except Exception as e:
        logger.error(f"An error ocurred while copying files: {str(e)}")
        return False
    return result

def run_calibration(calibration_folder_path: str, include_debug: bool = False) -> None:
    """
    Runs the calibration process in the specified folder.

    Args:
        calibration_folder_path (str): The path to the folder containing the calibration files.
        include_debug (bool, optional): Whether to include debug information in the logs. Defaults to False.

    Returns:
        None
    """
    try:
        # Change directory to the calibration folder
        os.chdir(calibration_folder_path)
        current_path = os.getcwd()

        logger.debug(f"The current path is {current_path}")

        # Check if all files exist
        if isLinux():
            command = f'gams CSV_CALIBRATION_MODEL.gms {"o=/dev/null" if not include_debug else ""}'
        else:
            command = 'cmd.exe /c "gams CSV_CALIBRATION_MODEL.gms"'

        # Run gams command
        logger.debug(f"Calibration is starting now ({calibration_folder_path})") 
        
        with DelayedLog(logger= logger, retention_size=50, retention_time=30) as dl:
            with subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1, universal_newlines=True, text=True) as result:
                for line in result.stdout:
                    if include_debug:
                        dl.addMessage(message = f'{line}\n', level = LogLevel.DEBUG)
        
        if result.returncode == 0:
            # Command ran successfully
            # For subj1 proper treatment, we need to run twice, therefore we do it
            with DelayedLog(logger= logger, retention_size=50, retention_time=30) as dl:
                with subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1, universal_newlines=True, text=True) as result:
                    for line in result.stdout:
                        if include_debug:
                            dl.addMessage(message = f'{line}\n', level = LogLevel.DEBUG)
            if result.returncode == 0:
                logger.debug(f"Calibration completed successfully ({calibration_folder_path})")
            else: 
                # Command encountered an error
                logger.error("Some problem during calibration phase (after proper subj1 creation). Full log attached")
                raise AgricoreTaskError("Failed to run calibration. Return code was not 0")    
        else:
            # Command encountered an error
            logger.error("Some problem during calibration phase. Full log attached")
            raise AgricoreTaskError("Failed to run calibration. Return code was not 0")
    except Exception as e:
        logger.error("Failed to run calibration. {0}".format(repr(e)))
        raise AgricoreTaskError("Failed to run calibration. {0}".format(repr(e)))
    
def check_xls_files(file_path:str, files:List[str]) -> None:
    """
    Checks if the specified Excel files exist in the given file path.

    Args:
        file_path (str): The path to the folder where the files are expected to be located.
        files (List[str]): A list of filenames to check for existence.

    Returns:
        None

    Raises:
        AgricoreTaskError: If any of the specified files do not exist in the given folder.
    """
    for excel_file_path in files:
        full_file_path = os.path.join(file_path, excel_file_path)
        if not os.path.exists(full_file_path):
            logger.error(f"File '{excel_file_path}' does not exist in the calibration folder.")
            raise AgricoreTaskError(f"File '{excel_file_path}' does not exist in the calibration folder.")
        

def process_short_term_simulation_output(simulation_folder_path: str, scenario_descriptor: str, policies: List[PolicyJsonDTO])-> Union[List[ValueFromSPDTO], None]:
    """
    Processes the short-term simulation output from the specified folder.

    Args:
        simulation_folder_path (str): The path to the folder containing simulation output files.
        scenario_descriptor (str): The descriptor for the scenario to process within the simulation output.
        policies (List[PolicyJsonDTO]): A list of policy objects containing information about policies applied.

    Returns:
        Union[List[ValueFromSPDTO], None]: A list of simulation results (ValueFromSPDTO objects) or None if there was an error.

    Raises:
        FileNotFoundError: If any required file is not found.
        PermissionError: If there is a permission issue accessing any file.
    """
    logger.debug(f"Processing short term simulation output - {simulation_folder_path}")

    farms_output_file = os.path.join(simulation_folder_path, 'farms_output.csv')
    farms_rent_file = os.path.join(simulation_folder_path, 'farms_VV.csv')
    rental_price_file = os.path.join(simulation_folder_path, 'rental_price.csv')
    coupled_subsidies_file = os.path.join(simulation_folder_path, 'coupled_subsidies_output.csv')
    results: Dict[int, ValueFromSPDTO] = {}

    try:
        green_policy = next((x for x in policies if x.modelLabel == "Greening"), None)
        basic_policy = next((x for x in policies if x.modelLabel == "Basic"), None)
        # correct the previous line so it cannot fail if the list comprehension does not find the policy
        with open(farms_output_file, "r") as outputFile:
            reader = csv.reader(outputFile)
            next(reader)  # Skip the header row

            for row in reader:
                farm_id = int(row[0])
                parameter = row[1]
                crop = row[2]
                scenario = row[3]
                value = float(row[4])
                
                if scenario == scenario_descriptor:
                
                    if farm_id not in results:
                        results[farm_id] = ValueFromSPDTO(
                            farmId=farm_id,
                            agriculturalLand=0,
                            totalCurrentAssets=0,
                            farmNetIncome=0,
                            farmGrossIncome=0,
                            crops={},
                            rentBalanceArea = 0,
                            totalVariableCosts = 0,
                            subsidies=[],
                            greeningSurface=0,
                            rentedInLands=[]
                        )

                    farm_entry = results[farm_id]
                    
                    if parameter == "GM":
                        # This does not seem correct. GM = GrossMargin which is not the same as farmNetIncome nor farmGrossIncome
                        farm_entry.farmNetIncome = value * 10000
                        farm_entry.farmGrossIncome = value * 10000
                    elif parameter == "liq":
                        farm_entry.totalCurrentAssets = value
                    elif parameter == "uaa":
                        farm_entry.agriculturalLand = value
                    elif parameter == "costv":
                        farm_entry.totalVariableCosts = value
                    elif parameter == "rented_land":
                        farm_entry.rentBalanceArea = value
                    elif parameter == "P_GREEN":
                        farm_entry.subsidies.append(FarmYearSubsidyDTO(
                            farmId = farm_id,
                            yearNumber = 0,
                            value = value,
                            policyIdentifier = green_policy.policyIdentifier
                        ))
                    elif parameter == "P_BASIC":
                        farm_entry.subsidies.append(FarmYearSubsidyDTO(
                            farmId = farm_id,
                            yearNumber = 0,
                            value = value,
                            policyIdentifier = basic_policy.policyIdentifier
                        ))
                    elif parameter == "P_PENSION":
                        pass 
                    elif parameter == "sbpa":
                        farm_entry.greeningSurface = value
                    elif crop not in farm_entry.crops and crop != "XXX":
                        farm_entry.crops[crop] = CropDataDTO (
                            cropProductiveArea=0,
                            cropVariableCosts=0,
                            quantitySold=0,
                            quantityUsed=0,
                            cropSellingPrice=0,
                            coupledSubsidy=0,
                            rebreedingCows=0,
                            dairyCows=0,
                            uaa=0)
                    elif parameter=="rebreeding_cows" or parameter=="dairy_cows":
                        if "MILK" not in farm_entry.crops:
                            farm_entry.crops["MILK"] = CropDataDTO (
                                cropProductiveArea=0,
                                cropVariableCosts=0,
                                quantitySold=0,
                                quantityUsed=0,
                                cropSellingPrice=0,
                                coupledSubsidy=0,
                                rebreedingCows=0,
                                dairyCows=0,
                                uaa=0)
                        if parameter == "rebreeding_cows":
                            farm_entry.crops["MILK"].rebreedingCows = value
                        elif parameter == "dairy_cows":
                            farm_entry.crops["MILK"].dairyCows = value
                            
                    if crop in farm_entry.crops:
                        if parameter == "supv":
                            farm_entry.crops[crop].cropProductiveArea = round(value)
                            farm_entry.crops[crop].uaa = value
                        elif parameter == "prodv":
                            farm_entry.crops[crop].quantitySold = value * 1000
                        elif parameter == "proda":
                            farm_entry.crops[crop].quantityUsed = value * 1000
                        elif parameter == "prezv":
                            farm_entry.crops[crop].cropSellingPrice = value * 10
        
        rental_prices = {}
        with open(rental_price_file, "r") as rentPriceFile:
            reader = csv.reader(rentPriceFile)
            for row in reader:
                farm_id = int(row[0])
                rent_price = float(row[1])
                rental_prices[farm_id] = rent_price
                
        with open(farms_rent_file, "r") as rentFile:
            reader = csv.reader(rentFile)
            next(reader)  # Skip the header row

            for row in reader:
                origin_farm_id = int(row[0])
                destination_farm_id = int(row[1])
                scenario = row[2]
                ha_rented = float(row[3])
                if scenario == scenario_descriptor:
                    
                    value_rented = rental_prices[origin_farm_id] * ha_rented
                
                    if destination_farm_id not in results:
                        results[destination_farm_id] = ValueFromSPDTO(
                            farmId=destination_farm_id,
                            agriculturalLand=0,
                            totalCurrentAssets=0,
                            farmNetIncome=0,
                            farmGrossIncome=0,
                            crops={},
                            rentBalanceArea = 0,
                            totalVariableCosts = 0,
                            subsidies=[],
                            greeningSurface=0,
                            rentedInLands=[]
                        )

                    farm_entry = results[destination_farm_id]
                    
                    farm_entry.rentedInLands.append(LandRentDTO(
                        yearId = 0,
                        originFarmId = origin_farm_id,
                        destinationFarmId = destination_farm_id,
                        rentArea = ha_rented,
                        rentValue = value_rented
                    ))
        
        if os.path.isfile(coupled_subsidies_file):         
            with open(coupled_subsidies_file, "r") as csubsidiesFile:
                reader = csv.reader(csubsidiesFile)
                next(reader)  # Skip the header row
                for row in reader:
                    if scenario == scenario_descriptor:
                        farm_id = int(row[0])
                        policy = row[1]
                        product_group_name = row[2]
                        scenario = row[3]
                        value = float(row[4])
                        
                        if farm_id not in results:
                            results[farm_id] = ValueFromSPDTO(
                                farmId=farm_id,
                                agriculturalLand=0,
                                totalCurrentAssets=0,
                                farmNetIncome=0,
                                farmGrossIncome=0,
                                crops={},
                                rentBalanceArea = 0,
                                totalVariableCosts = 0,
                                subsidies=[],
                                greeningSurface=0,
                                rentedInLands=[]
                            )

                        farm_entry = results[farm_id]
                        
                        existing_subsidy = None
                        for subsidy in farm_entry.subsidies:
                            if subsidy.policyIdentifier == policy:
                                existing_subsidy = subsidy
                                break
                        if existing_subsidy is None:
                            farm_entry.subsidies.append(FarmYearSubsidyDTO(
                                farmId = farm_id,
                                yearNumber = 0,
                                value = value,
                                policyIdentifier = policy
                            ))
                        else:
                            existing_subsidy.value = existing_subsidy.value + value
        else:
            logger.warning(f"File {coupled_subsidies_file} not found")
        
        # Lest clean the entries where no land or production appears (but still may have selling price because its default)
        to_be_deleted = []
        for farm_id in results.keys():
            farm_entry = results[farm_id] 
            for crop_name in farm_entry.crops:
                if crop_name != "MILK":
                    crop = farm_entry.crops[crop_name]
                    if crop.quantitySold + crop.quantityUsed + crop.cropProductiveArea == 0:
                        to_delete = {"farm_id": farm_id, "crop_name": crop_name}
                        to_be_deleted.append(to_delete)
        for entry in to_be_deleted:
            del(results[entry["farm_id"]].crops[entry["crop_name"]])
        return list(results.values())
    except FileNotFoundError:
        logger.error(f"File not found: {farms_output_file}")
        return None
    except PermissionError:
        logger.error(f"Permission denied: {farms_output_file}")
        return None

    
def run_simulation(simulation_folder_path: str, scenario_descriptor:str, include_debug: bool = False) -> None:
    """
    Runs the simulation in the specified folder using GAMS.

    Args:
        simulation_folder_path (str): The path to the folder containing simulation files.
        scenario_descriptor (str): Descriptor for the scenario to simulate within the folder.
        include_debug (bool, optional): Whether to include debug information in logs. Defaults to False.

    Returns:
        None

    Raises:
        AgricoreTaskError: If the simulation fails.
    """
    logger.debug(f"Launching simulation in folder {simulation_folder_path}")

    try:
        # Change directory to the calibration folder
        os.chdir(simulation_folder_path)
        current_path = os.getcwd()

        logger.debug(f"The current path is {current_path}")
        if isLinux():
            command = f'gams CSV_SIMULATION.gms {"o=/dev/null" if not include_debug else ""}'
        else:
            command = f'cmd.exe /c "gams CSV_SIMULATION.gms"'
            
        logger.debug(f"ST Simulation is starting now ({simulation_folder_path})")

        # Run gams command
        with DelayedLog(logger= logger, retention_size=50, retention_time=30) as dl:
            with subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, bufsize=1, universal_newlines=True, text=True) as result:
                for line in result.stdout:
                    if include_debug:
                        dl.addMessage(message = f'{line}\n', level = LogLevel.DEBUG)
        if result.returncode == 0:
            # Command ran successfully
            logger.debug(f"ST Simulation completed successfully ({simulation_folder_path})")
            return 
        else:
            # Command encountered an error
            logger.error("Some problem during simulation phase")
            raise AgricoreTaskError("Failed to run simulation. Return code was not 0")
    except Exception as e:
        logger.error("Failed to run simulation at {0}. {1}".format(simulation_folder_path, repr(e)))
        raise AgricoreTaskError("Failed to run simulation {0}. {1}".format(simulation_folder_path, repr(e)))
    
def initialize_folder_for_short_term(simulationRunId: int, branch: str) -> None:
    """
    Initializes the folder for short-term execution based on the given simulation run ID and branch.

    Args:
        simulationRunId (int): The ID of the simulation run.
        branch (str): The branch name to download from.

    Returns:
        None

    Raises:
        Exception: If any error occurs during initialization.
    """
    logger.info(f"Initializing folder for ST execution for simulationRunId {simulationRunId}")
    try:
        dir_suffix = 'st_model_sr_' + str(simulationRunId)
        prepath = os.getenv('ST_DEPLOY_DIR')
        working_dir = os.path.join(prepath, dir_suffix)
        GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
        ST_MODEL_REPOSITORY = os.getenv('ST_MODEL_REPOSITORY')
        download_and_extract_branch(GITHUB_TOKEN, ST_MODEL_REPOSITORY, branch, working_dir, 
                                    tmp_dir = prepath + "/latest_" + dir_suffix, tmp_download_file=prepath + "/latest_"+ dir_suffix+".tar")
        common_data_dir = os.path.join(working_dir, "COMMON")
        common_files = os.listdir(common_data_dir)
        calibration_dir = os.path.join(working_dir, "MODEL_CSV_CALIBRATION")
        simulation_dir = os.path.join(working_dir, "MODEL_CSV_SIMULATION")
        for common_file in common_files:
            if os.path.isfile(os.path.join(common_data_dir,common_file)):
                shutil.copy2(os.path.join(common_data_dir,common_file), calibration_dir)
                shutil.copy2(os.path.join(common_data_dir,common_file), simulation_dir)
                logger.trace(f"Copying common file {common_file} to calibration and simulation folders")
            else:
                logger.trace(f"Skipping common entry {common_file} as it is not a file")
    except Exception as e:
        raise e

def fail_task(task: celery.Task, ex: Exception, message: str, launch_ignore: bool = False):
    """
    Fails the given Celery task and raises an exception.

    Args:
        task (celery.Task): The Celery task to fail.
        ex (Exception): The exception object.
        message (str): Custom error message.
        launch_ignore (bool, optional): Whether to ignore the failure and continue processing. Defaults to True.

    Raises:
        Ignore: If launch_ignore is True, raises Ignore to continue processing.
        TaskError: Always raises TaskError to indicate task failure.
    """
    task.update_state(
        state = states.FAILURE,
        meta={
                'exc_type': type(ex).__name__,
                'exc_message': traceback.format_exc().split('\n'),
                'custom': message
            })
    if launch_ignore:
        raise Ignore()
    else:
        raise TaskError()

def upload_file_s3(file_name, object_name=None):
    """Upload a file to the configured S3 bucket
    :param file_name: File to upload
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    
    bucket = os.getenv('S3_BUCKET')
    url = os.getenv('S3_ENDPOINT_URL')
    s3_ak = os.getenv('S3_ACCESS_KEY_ID')
    s3_sk = os.getenv('S3_SECRET_ACCESS_KEY')
    s3_region_name = os.getenv('S3_REGION_NAME')

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client(endpoint_url=url, aws_access_key_id=s3_ak, aws_secret_access_key=s3_sk, service_name="s3", region_name=s3_region_name)
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
        logger.debug(f"Succesful upload of {file_name} file")
    except ClientError as e:
        logger.error(f"Failed to upload {file_name} file. {e}")
        return False
    return True

def download_file_s3(file_name, save_path, object_name=None) -> None:
    """Download a file from the configured S3 bucket
    :param file_name: File to download
    :param save_path: full path to save the file including new file name
    :return: True if file was downloaded, else False
    """
    
    bucket = os.getenv('S3_BUCKET')
    url = os.getenv('S3_ENDPOINT_URL')
    s3_ak = os.getenv('S3_ACCESS_KEY_ID')
    s3_sk = os.getenv('S3_SECRET_ACCESS_KEY')
    s3_region_name = os.getenv('S3_REGION_NAME')

    # Download the file
    s3_client = boto3.client(endpoint_url=url, aws_access_key_id=s3_ak, aws_secret_access_key=s3_sk, service_name="s3", region_name=s3_region_name)
    try:
        response = s3_client.download_file(bucket, file_name, save_path)
        logger.debug(f"Succesful download of {file_name} file")
    except ClientError as e:
        logger.error(f"Failed to download {file_name} file. {e}")
        raise e

def extract_tar(file_path, dest_path) -> None: 
    """
    Extracts the contents of a tar file to the specified destination path.

    Args:
        file_path (str): The path to the tar file.
        dest_path (str): The destination path to extract the contents to.

    Returns:
        None

    Raises:
        Exception: If any error occurs during extraction.
    """
    try:
        with tarfile.open(file_path, "r:gz") as tar:
            tar.extractall(dest_path)
            logger.debug(f"Succesful untar of {file_path} file")

    except Exception as e:
        logger.error(f"Failed to extract tar file {file_path}. {e}")
        raise e
    
def create_directory_tar(dir_path, tar_path) -> None:
    """
    Creates a tar file from the specified directory path.

    Args:
        dir_path (str): The path to the directory to create the tar file from.
        tar_path (str): The path to the tar file to create.

    Returns:
        None

    Raises:
        Exception: If any error occurs during tar creation.
    """
    try:
        with tarfile.open(tar_path, "w:gz") as tar:
            tar.add(dir_path, arcname=os.path.basename(dir_path))
            logger.debug(f"Succesful tar of {dir_path} directory")
    except Exception as e:
        logger.error(f"Failed to create tar file {tar_path}. {e}")
        raise e

def manage_failed_task(task: celery.Task, raise_exception: bool = True) -> NoReturn:
    logger.debug(f"Task failed. Result type: {type(task.result)}, Traceback type: {type(task.traceback)}")
    if isinstance(task.result, Exception):
        if isinstance(task.traceback, TracebackType):
            logger.debug(f"Error in {task.name} task: {task.traceback.format_exc()}")
        if (raise_exception):
            raise task.result
    else:
        if (raise_exception):
            raise Exception("Error while scheduling short term simulation. Further details not available")
from typing import Optional
from pydantic import BaseModel


class ShortTermModelParameters (BaseModel):

    """
    A class used to represent Short Term Model Parameters

    ...

    Attributes
    ----------
    populationId : int
        The population ID
    yearId : int
        The year ID
    year : int
        The year number
    simulationRunId : int
        The simulation run ID
    branch : str
        branch name of the short term model code
    excludedFarmsIds : list[int]
        A list of excluded farm IDs
    numberOfYears : int
        number of years to compute beyond the initial year
    initialYear : int
        initial year to begin the process
    includeDebug : bool
        Whether to include debugging (default is False)
    queueSuffix : Optional[str]
        suffix of the celery queue (default is None)
    partitionBeingProcessed : Optional[int]
        (default is None)
    scenarioDescriptor : str
        description of the scenario (default is "s_em")
    compress : Optional[bool]
        Whether to compress responses for improved veolicity (default is False)

    Methods
    -------
    None
    """
    populationId: int
    yearId: int
    year: int
    simulationRunId: int
    branch: str
    excludedFarmsIds: list[int]
    numberOfYears: int
    initialYear: int
    includeDebug: bool = False
    queueSuffix: Optional[str] = None
    partitionBeingProcessed: Optional[int] = None
    scenarioDescriptor: str = "s_em"
    compress: Optional[bool] = False


#help(ShortTermModelParameters)

class LongTermOptimizationParameters (BaseModel):
    """
    A class used to represent Long Term Optimization Parameters

    ...

    Attributes
    ----------
    populationId : int
        The population ID
    yearId : int
        The year ID
    ignoreLP : Optional[bool]
        Whether to ignore LP (default is False)
    ignoreLMM : Optional[bool]
        Whether to ignore LMM (default is False)
    branch : str
        The branch
    simulationRunId : int
        The simulation run ID
    yearNumber : int
        The year number
    compress : Optional[bool]
        Whether to compress (default is False)

    Methods
    -------
    None
    """
    populationId: int
    yearId: int
    ignoreLP: Optional[bool] = False
    ignoreLMM: Optional[bool] = False
    branch: str
    simulationRunId: int
    yearNumber: int
    compress: Optional[bool] = False
    
class ShortTermCalibrationData(BaseModel):
    """
    A class used to represent Short Term Calibration Data

    ...

    Attributes
    ----------
    a : int
        
    excludedFarmsIds : list[int]
        A list of excluded farm IDs

    Methods
    -------
    None
    """
    a: int
    excludedFarmsIds: list[int]
    
class Year (BaseModel):
    """
    A class used to represent a Year

    ...

    Attributes
    ----------
    yearId : Optional[int]
        The year ID (default is None)
    yearNumber : int
        The year number
    populationId : int
        The population ID

    Methods
    -------
    None
    """
    yearId: Optional[int] = None
    yearNumber: int
    populationId: int

    
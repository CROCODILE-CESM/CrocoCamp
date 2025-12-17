"""Base ModelAdapter class to normalize model input."""

from abc import ABC, abstractmethod

class ModelAdapter(ABC):
    """Base class for all model normalizations

    Provides common functionality for model input normalization.
    """

    def __init__(self) -> None:

        # Assign time_variable_name
        return False

    @abstractmethod
    def get_required_config_keys(self) -> List[str]:
        """Return list of required configuration keys.
        
        Returns:
            List of required configuration key names
        """
    
        return False

    @abstractmethod
    def validate_run_arguments(self) -> List[str]:
        """Validate that model can use provided arguments specified with
        workflow.run()
        
        Raise:
            ValueError if provided argument is not compatible and is set to True
            Warning if provided argument is not compatible but is set to False

        """
    
        return False

    @abstractmethod
    def get_common_model_keys(self) -> List[str]:
        """Return list of keys that are common to all input.nml files for this
        model
        
        Returns:
            List of common key

        """
    
        return False


    @abstractmethod
    def get_ds(self) -> xarray.dataset:
        """Return xarray dataset for specific model"""

        return False

    @abstractmethod
    def rename_time_variable(self) -> xarray.dataset:
        """Rename time variable in dataset to common name for workflow

        Returns:
           Updated xarray dataset

        """

        return False

    @abstractmethod
    def convert_units(self) -> dask.Series:
        """Convert observation or model units to match workflow
        
        Returns:
            Converted dataseries

        """
    
        return False



"""ModelAdapter class to normalize ROMS model input."""

from typing import Any, Dict, List
import dask.dataframe as dd
import xarray as xr

from . import model_adapter

class ModelAdapterROMS(model_adapter.ModelAdapter):
    """Base class for all model normalizations

    Provides common functionality for model input normalization.
    """

    def __init__(self) -> None:

        # Assign time_variable_name
        return

    def get_required_config_keys(self) -> List[str]:
        """Return list of required configuration keys.
        
        Returns:
            List of required configuration key names
        """
    
        return [
            'model_files_folder', 
            'obs_seq_in_folder', 
            'output_folder',
            'roms_filename',
            'perfect_model_obs_dir', 
            'parquet_folder'
        ]
    
    def validate_run_arguments(self) -> None:
        """Validate that model can use provided arguments specified with
        workflow.run()
        
        Raise:
            ValueError if provided argument is not compatible and is set to True
            Warning if provided argument is not compatible but is set to False

        """
    
        return False
    
    def get_common_model_keys(self) -> List[str]:
        """Return list of keys that are common to all input.nml files for this
        model
        
        Returns:
            List of common key

        """
    
        return False


    def get_ds(self) -> xr.Dataset:
        """Return xarray dataset for specific model"""

        return False

    def rename_time_variable(self) -> xr.Dataset:
        """Rename time variable in dataset to common name for workflow

        Returns:
           Updated xarray dataset

        """

        return False

    def convert_units(self) -> dd.Series:
        """Convert observation or model units to match workflow
        
        Returns:
            Converted dataseries

        """
    
        return False



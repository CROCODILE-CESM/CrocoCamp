"""ModelAdapter class to normalize MOM6 model input."""

from contextlib import contextmanager
from collections.abc import Iterator
from typing import Any, Dict, List
import pandas as pd
import xarray as xr

from . import model_adapter

class ModelAdapterMOM6(model_adapter.ModelAdapter):
    """Base class for all model normalizations

    Provides common functionality for model input normalization.
    """

    def __init__(self) -> None:

        # Assign ocean model name
        self.ocean_model = "MOM6"
        # Assign time_variable_name
        self.time_varname = "time"
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
            'template_file', 
            'static_file', 
            'ocean_geometry',
            'perfect_model_obs_dir', 
            'parquet_folder'
        ]
    
    def get_common_model_keys(self) -> List[str]:
        """Return list of keys that are common to all input.nml files for this
        model
        
        Returns:
            List of common key

        """

        return [
            'model_files_folder', 
            'obs_seq_in_folder', 
            'output_folder',
            'roms_filename',
            'perfect_model_obs_dir', 
            'parquet_folder'
        ]

    @contextmanager
    def open_dataset_ctx(self, path: str) -> Iterator[xr.Dataset]:
        """Open a dataset performing very few MOM6-dependent operations and
        close it
        """
        
        ds = xr.open_dataset(
            path,
            decode_times=False
        )

        try:
            # Fix calendar as xarray does not read it consistently with ncviews
            ds[self.time_varname].attrs['calendar'] = 'proleptic_gregorian'
            ds = xr.decode_cf(ds, decode_timedelta=True)
            ds = self.rename_time_varname(ds)
            yield ds
        finally:
            ds.close()

    def convert_units(self, df) -> pd.DataFrame:
        """Convert observation or model units to match workflow
        
        Returns:
            Converted dataframe

        """

        # MOM6 is in PSU
        # DART's obs_seq are in PSU/1000
        # DART's pmo for MOM6 converts units
        # In the future DART might move to PSU
    
        return df

    def validate_run_arguments(self) -> None:
        """Validate that model can use provided arguments specified with
        workflow.run()
        
        Raise:
            ValueError if provided argument is not compatible and is set to True
            Warning if provided argument is not compatible but is set to False

        """
    
        return False
    

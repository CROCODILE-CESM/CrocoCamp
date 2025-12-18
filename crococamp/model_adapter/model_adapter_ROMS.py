"""ModelAdapter class to normalize ROMS model input."""

from contextlib import contextmanager
from collections.abc import Iterator
from typing import Any, Dict, List
import dask.dataframe as dd
import xarray as xr

from . import model_adapter

class ModelAdapterROMS(model_adapter.ModelAdapter):
    """Base class for all model normalizations

    Provides common functionality for model input normalization.
    """

    def __init__(self) -> None:

        # Assign time_varname_name
        self.time_varname = "ocean_time"
        return

    def get_required_config_keys(self) -> List[str]:
        """Return list of required configuration keys.
        
        Returns:
            List of required configuration key names
        """
    
        return [
            'template_file',
            'static_file',
            'ocean_geometry',
            'model_state_variables',
            'layer_name'
        ]
    
    def get_common_model_keys(self) -> List[str]:
        """Return list of keys that are common to all input.nml files for this
        model
        
        Returns:
            List of common key

        """
    
        return [
            'roms_filename',
            'variables',
            'debug'
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
            ds = xr.decode_cf(ds, decode_timedelta=True)
            ds = self.rename_time_varname()
            yield ds
        finally:
            ds.close()

    def convert_units(self) -> dd.Series:
        """Convert observation or model units to match workflow
        
        Returns:
            Converted dataseries

        """
    
        return False

    def validate_run_arguments(self) -> None:
        """Validate that model can use provided arguments specified with
        workflow.run()
        
        Raise:
            ValueError if provided argument is not compatible and is set to True
            Warning if provided argument is not compatible but is set to False

        """
    
        return False
    

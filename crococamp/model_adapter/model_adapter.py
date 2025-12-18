"""Base ModelAdapter class to normalize model input."""

from abc import ABC, abstractmethod
from contextlib import contextmanager
from collections.abc import Iterator
from typing import Any, Dict, List

import dask.dataframe as dd
import xarray as xr

class ModelAdapter(ABC):
    """Base class for all model normalizations

    Provides common functionality for model input normalization.
    """

    def __init__(self) -> None:

        # Assign time_varname_name
        return False

    @contextmanager
    def open_dataset_ctx(self, path: str) -> Iterator[xr.Dataset]:
        """Open a dataset and guarantee it is closed."""

        ds = xr.open_dataset(path, decode_timedelta=True)
        try:
            yield ds
        finally:
            ds.close()

    @abstractmethod
    def get_required_config_keys(self) -> List[str]:
        """Return list of required configuration keys.
        
        Returns:
            List of required configuration key names
        """
    
        return

    @abstractmethod
    def validate_run_arguments(self) -> None:
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


    def rename_time_varname(self) -> xr.Dataset:
        """Rename time variable in dataset to common name for workflow

        Returns:
           Updated xarray dataset

        """

        ds = ds.rename({self.time_varname: "time"})

        return ds

    @abstractmethod
    def convert_units(self) -> dd.Series:
        """Convert observation or model units to match workflow
        
        Returns:
            Converted dataseries

        """
    
        return False



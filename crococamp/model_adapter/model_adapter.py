"""Base ModelAdapter class to normalize model input."""

from __future__ import annotations
from typing import TYPE_CHECKING, Any, Dict, List
if TYPE_CHECKING:
    from crococamp.workflows.types import RunOptions

from abc import ABC, abstractmethod
from contextlib import contextmanager
from collections.abc import Iterator

import dask.dataframe as dd
import xarray as xr

from dataclasses import dataclass

@dataclass(frozen=True)
class ModelAdapterCapabilities:
    supports_trim_obs: bool = True
    supports_no_matching: bool = True
    supports_force_obs_time: bool = True


class ModelAdapter(ABC):
    """Base class for all model normalizations

    Provides common functionality for model input normalization.
    """

    # run arguments
    capabilities: ModelAdapterCapabilities = ModelAdapterCapabilities()

    def __init__(self) -> None:
        """Initialize base ModelAdapter.

        Note: This is an abstract base class. Subclasses must override this
        method to set:
        - self.ocean_model: Name of the ocean model (str)
        - self.time_varname: Name of the time variable in model files (str)
        """
        pass  # Subclasses must implement

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

    def validate_run_options(self, opts: RunOptions) -> None:
        """Validate that model can use provided arguments specified with
        workflow.run()
        
        Raise:
            ValueError if provided argument is not compatible and is set to True
            Warning if provided argument is not compatible but is set to False

        """
        
        cap = self.capabilities
        if opts.trim_obs and not cap.supports_trim_obs:
            raise NotImplementedError(
                f"{self.ocean_model} adapter does not support "
                f"observation files trimming."
            )
        if opts.no_matching and not cap.supports_no_matching:
            raise NotImplementedError(
                f"{self.ocean_model} adapter does not support "
                f"skipping time matching."
            )
        if opts.force_obs_time and not cap.supports_force_obs_time:
            raise NotImplementedError(
                f"{self.ocean_model} adapter does not support "
                f"assigning the observations reference time to model files."
            )       


    @abstractmethod
    def get_common_model_keys(self) -> List[str]:
        """Return list of keys that are common to all input.nml files for this
        model
        
        Returns:
            List of common key

        """
    
        return False


    def rename_time_varname(self, ds: xr.Dataset) -> xr.Dataset:
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



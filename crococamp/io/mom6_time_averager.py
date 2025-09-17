"""MOM6-specific time-averaging implementation.

This module provides the MOM6TimeAverager class that extends the abstract
TimeAverager base class with MOM6-specific functionality such as native
interval detection from average_DT variable and MOM6 filename conventions.
"""

import os
from pathlib import Path
from typing import List
import warnings

import numpy as np
import pandas as pd
import xarray as xr

from .time_averager import TimeAverager


class MOM6TimeAverager(TimeAverager):
    """MOM6-specific time-averaging utility.
    
    Extends TimeAverager with MOM6-specific functionality:
    - Native interval detection from average_DT variable
    - MOM6 filename conventions
    - MOM6-specific variable handling
    """
    
    def _detect_native_interval(self, dataset: xr.Dataset) -> pd.Timedelta:
        """Detect the native time interval from MOM6 dataset.
        
        Tries multiple methods in order:
        1. average_DT variable (with proper unit handling)
        2. DT global attribute
        3. Time coordinate differences
        4. Default fallback with warning
        
        Args:
            dataset: Input MOM6 dataset
            
        Returns:
            Native time interval as pandas Timedelta
        """
        # Method 1: Try average_DT variable with proper unit handling
        if 'average_DT' in dataset.variables:
            avg_dt = dataset['average_DT']
            
            # Get the first value
            if avg_dt.size > 0:
                dt_value = avg_dt.isel(time=0).values
                
                # Handle different data types
                if isinstance(dt_value, np.timedelta64):
                    # Already a timedelta - convert to pandas Timedelta
                    return pd.Timedelta(dt_value)
                else:
                    # Numeric value - get units and convert
                    units = avg_dt.attrs.get('units', 'days')
                    dt_float = float(dt_value)
                    
                    # Convert based on units
                    if units == 'days':
                        return pd.Timedelta(days=dt_float)
                    elif units == 'hours':
                        return pd.Timedelta(hours=dt_float)
                    elif units == 'seconds':
                        return pd.Timedelta(seconds=dt_float)
                    else:
                        raise ValueError(f"Unknown average_DT units '{units}', expected 'days', 'hours', or 'seconds'")
        
        # Method 2: Try time coordinate differences (removed DT global attribute method)
        if 'time' in dataset.coords:
            warnings.warn("Could not find average_DT variable, trying time coordinate differences instead", 
                         UserWarning)
            time_coord = dataset.coords['time']
            if len(time_coord) > 1:
                # Calculate difference between first two time points
                time_diff = time_coord.isel(time=1) - time_coord.isel(time=0)
                # Convert to pandas Timedelta safely
                if hasattr(time_diff, 'values'):
                    # Convert numpy timedelta64 to pandas Timedelta
                    time_diff_ns = pd.Timedelta(time_diff.values)
                    return time_diff_ns
                else:
                    # Handle cftime objects
                    time1 = pd.Timestamp(time_coord.isel(time=1).values)
                    time0 = pd.Timestamp(time_coord.isel(time=0).values) 
                    return time1 - time0
        
        # Raise error instead of using default fallback
        raise ValueError("Could not determine native time interval from MOM6 dataset. "
                        "No average_DT variable found and unable to calculate from time coordinates. "
                        "Please ensure the dataset has proper time interval information.")
    
    def _generate_output_filename(self, period_type: str, period_str: str) -> str:
        """Generate MOM6 output filename preserving original file relationships.
        
        The filename includes 'MOM6' identifier and preserves relation to
        original files as requested in the feedback.
        
        Args:
            period_type: Type of averaging period ('month', 'season', 'year', etc.)
            period_str: String representation of the period
            
        Returns:
            Output filename with MOM6 identifier
        """
        # Get base name from first input file to preserve relationships
        input_files = self._get_input_files()
        if input_files:
            first_file = Path(input_files[0])
            base_stem = first_file.stem
            
            # Try to extract a base name by removing common MOM6 patterns
            # Remove date patterns and common MOM6 suffixes
            import re
            base_clean = re.sub(r'_\d{4}_\d{2}_\d{2}', '', base_stem)  # Remove date patterns
            base_clean = re.sub(r'_\d{8}', '', base_clean)  # Remove YYYYMMDD patterns
            base_clean = re.sub(r'\.nc$', '', base_clean)  # Remove .nc extension
            
            if base_clean:
                base_name = base_clean
            else:
                base_name = "MOM6_output"
        else:
            base_name = "MOM6_output"
        
        # Generate filename with clear MOM6 identification
        filename = f"{base_name}_{period_type}_{period_str}.nc"
        
        return filename


def time_average_from_config(config_path: str) -> List[str]:
    """Convenience function to perform MOM6 time averaging from config file.
    
    Args:
        config_path: Path to configuration YAML file
        
    Returns:
        List of output file paths created
    """
    averager = MOM6TimeAverager(config_path)
    return averager.time_average()
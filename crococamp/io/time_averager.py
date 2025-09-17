"""Abstract base class for time-averaging of ocean model output files.

This module provides the abstract base class TimeAverager that implements
common time-averaging functionality that can be shared across different
ocean models (MOM6, ROMS, etc.) and gridded products.
"""

import glob
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import warnings

import dask.array as da
import numpy as np
import pandas as pd
import xarray as xr
import yaml

from ..utils.config import load_yaml_config, validate_file_pattern, check_or_create_folder
from ..io.file_utils import get_sorted_files


class TimeAverager(ABC):
    """Abstract base class for time-averaging ocean model output files.
    
    This class provides the common time-averaging functionality that can be
    shared across different ocean models. Subclasses should implement
    model-specific functionality like variable naming conventions,
    native interval detection, and filename generation.
    """
    
    def __init__(self, config_path: str) -> None:
        """Initialize the time averager with a YAML configuration file.
        
        Args:
            config_path: Path to YAML configuration file
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If required config keys are missing
        """
        self.config_path = config_path
        self.config = self._load_config(config_path)
        self._validate_config()
        
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load and validate configuration from YAML file.
        
        Args:
            config_path: Path to YAML configuration file
            
        Returns:
            Configuration dictionary
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config is invalid
        """
        # Use the utils config loader for general loading
        config = load_yaml_config(config_path)
        
        return config
        
    def _validate_config(self) -> None:
        """Validate that required configuration keys are present.
        
        Raises:
            ValueError: If required keys are missing
        """
        required_keys = ['input_files_pattern', 'output_directory', 'averaging_window']
        missing_keys = [key for key in required_keys if key not in self.config]
        
        if missing_keys:
            raise ValueError(f"Required configuration keys missing: {missing_keys}")
            
        # Validate file pattern here where it's required
        file_pattern = self.config.get('input_files_pattern')
        validate_file_pattern(file_pattern)
            
        # Validate averaging window format
        self._validate_averaging_window_config()
            
    def _validate_averaging_window_config(self) -> None:
        """Validate averaging window configuration format.
        
        Raises:
            ValueError: If averaging window format is invalid
        """
        window = self.config['averaging_window']
        
        # Handle legacy string format and new dict format
        if isinstance(window, str):
            # Legacy format - convert to new format
            if window in ['monthly', 'seasonal', 'yearly']:
                self.config['averaging_window'] = {
                    'type': 'predefined',
                    'window_size': window
                }
            else:
                raise ValueError(f"Invalid averaging window string: {window}")
        elif isinstance(window, dict):
            # New dict format - validate structure
            window_type = window.get('type')
            if window_type == 'predefined':
                if 'window_size' not in window:
                    raise ValueError("'window_size' required for predefined averaging windows")
                if window['window_size'] not in ['monthly', 'seasonal', 'yearly']:
                    raise ValueError(f"Invalid predefined window_size: {window['window_size']}")
            elif window_type == 'rolling':
                if 'window_size' not in window:
                    raise ValueError("'window_size' required for rolling averaging windows")
            elif window_type == 'custom':
                if 'freq' not in window:
                    raise ValueError("'freq' required for custom averaging windows")
            else:
                raise ValueError(f"Invalid averaging window type: {window_type}")
        else:
            raise ValueError("Invalid averaging window format - must be string or dict")
    
    def _get_input_files(self) -> List[str]:
        """Get sorted list of input files from glob pattern.
        
        Returns:
            Sorted list of input file paths
            
        Raises:
            FileNotFoundError: If no files match the pattern
        """
        pattern = self.config['input_files_pattern']
        
        # Extract directory and pattern from the full path
        directory = os.path.dirname(pattern) or '.'
        file_pattern = os.path.basename(pattern)
        
        # Use existing utility function
        files = get_sorted_files(directory, file_pattern)
        
        if not files:
            raise FileNotFoundError(f"No files found matching pattern: {pattern}")
            
        return files
        
    def _ensure_output_directory(self) -> None:
        """Create output directory if it doesn't exist."""
        output_dir = self.config['output_directory']
        check_or_create_folder(output_dir, "Output directory")
        
    @abstractmethod
    def _detect_native_interval(self, dataset: xr.Dataset) -> pd.Timedelta:
        """Detect the native time interval from the dataset.
        
        This method should be implemented by subclasses to handle
        model-specific ways of determining the native time interval.
        
        Args:
            dataset: Input dataset
            
        Returns:
            Native time interval as pandas Timedelta
        """
        
    def _generate_output_filename(self, period_type: str, period_str: str) -> str:
        """Generate output filename for a given period.
        
        Default implementation that can be overridden by subclasses for
        model-specific filename conventions.
        
        Args:
            period_type: Type of averaging period ('month', 'season', 'year', etc.)
            period_str: String representation of the period
            
        Returns:
            Output filename
        """
        # Default implementation - subclasses can override for model-specific naming
        filename = f"averaged_{period_type}_{period_str}.nc"
        return filename
    
    def _validate_averaging_window_vs_native(self, native_interval: pd.Timedelta, 
                                           requested_window: Dict[str, Any]) -> None:
        """Validate that averaging window is not shorter than native interval.
        
        Args:
            native_interval: Native time interval from model
            requested_window: Requested averaging window dict
            
        Raises:
            ValueError: If requested window is shorter than native interval
        """
        window_type = requested_window['type']
        
        if window_type == 'predefined':
            # Convert common window strings to approximate timedeltas for comparison
            window_size = requested_window['window_size']
            window_deltas = {
                'monthly': pd.Timedelta(days=30),  # Approximate
                'seasonal': pd.Timedelta(days=90),  # Approximate 
                'yearly': pd.Timedelta(days=365)   # Approximate
            }
            window_delta = window_deltas.get(window_size)
            
            if window_delta and window_delta < native_interval:
                raise ValueError(f"Requested averaging window '{window_size}' "
                               f"({window_delta}) is shorter than native model interval "
                               f"({native_interval})")
                               
        elif window_type in ['rolling', 'custom']:
            # For rolling windows, check the window size or frequency
            window_spec = requested_window.get('window_size') or requested_window.get('freq')
            if isinstance(window_spec, str):
                try:
                    window_delta = pd.Timedelta(window_spec)
                    if window_delta < native_interval:
                        raise ValueError(f"Requested averaging window '{window_spec}' "
                                       f"({window_delta}) is shorter than native model interval "
                                       f"({native_interval})")
                except (ValueError, TypeError):
                    # If we can't parse the window spec, skip validation with warning
                    warnings.warn(f"Could not validate averaging window '{window_spec}' "
                                 f"against native interval. Proceeding anyway.",
                                 UserWarning)

    def _perform_averaging(self, dataset: xr.Dataset, output_dir: str) -> List[str]:
        """Perform the requested averaging operation.
        
        This method coordinates the averaging based on the configuration
        and calls the appropriate specific averaging method.
        
        Args:
            dataset: Input dataset
            output_dir: Output directory
            
        Returns:
            List of created file paths
        """
        window_config = self.config['averaging_window']
        window_type = window_config['type']
        
        if window_type == 'predefined':
            window_size = window_config['window_size']
            if window_size == 'monthly':
                return self._perform_period_averaging(
                    dataset, output_dir, 'MS', 'month', self._format_monthly_period)
            elif window_size == 'seasonal':
                return self._perform_period_averaging(
                    dataset, output_dir, 'QS-DEC', 'season', self._format_seasonal_period)
            elif window_size == 'yearly':
                return self._perform_period_averaging(
                    dataset, output_dir, 'YS', 'year', self._format_yearly_period)
            else:
                raise ValueError(f"Unsupported predefined window size: {window_size}")
                
        elif window_type == 'rolling':
            return self._perform_rolling_averaging(dataset, output_dir, window_config)
            
        elif window_type == 'custom':
            return self._perform_custom_averaging(dataset, output_dir, window_config)
            
        else:
            raise ValueError(f"Unsupported averaging window type: {window_type}")

    def _perform_period_averaging(self, dataset: xr.Dataset, output_dir: str, 
                                 resample_rule: str, period_type: str,
                                 period_formatter = None) -> List[str]:
        """Generalized method to perform period-based averaging.
        
        Args:
            dataset: Input dataset
            output_dir: Output directory
            resample_rule: pandas resample rule (e.g., 'MS', 'QS-DEC', 'YS')
            period_type: Type of period for filename ('month', 'season', 'year')
            period_formatter: Optional function to format period labels to strings
            
        Returns:
            List of created file paths
        """
        output_files = []
        
        # Group by the specified period
        period_groups = dataset.resample(time=resample_rule)
        
        for label, group in period_groups:
            if len(group.time) == 0:
                continue
                
            # Calculate period average
            period_avg = group.mean(dim='time', keep_attrs=True)
            
            # Update time coordinate to represent the period - keep as array
            period_avg = period_avg.expand_dims('time')
            period_avg = period_avg.assign_coords(time=[label])
            
            # Generate period string using formatter or default
            if period_formatter:
                period_str = period_formatter(label)
            else:
                # Default formatter handles cftime objects
                if hasattr(label, 'strftime'):
                    period_str = label.strftime('%Y-%m-%d')
                else:
                    period_str = str(label).split('T')[0]
            
            filename = self._generate_output_filename(period_type, period_str)
            filepath = os.path.join(output_dir, filename)
            
            # Save to file with proper encoding for timedelta variables
            encoding = {}
            if 'average_DT' in period_avg.variables:
                # Encode timedelta as float64 in days
                encoding['average_DT'] = {
                    'dtype': 'float64',
                    'units': 'days'
                }
            
            period_avg.to_netcdf(filepath, encoding=encoding)
            output_files.append(filepath)
            
        return output_files

    def _format_monthly_period(self, label) -> str:
        """Format monthly period label."""
        if hasattr(label, 'year') and hasattr(label, 'month'):
            return f"{label.year:04d}-{label.month:02d}"
        else:
            # Convert to string and parse
            ts_str = str(label)
            timestamp = pd.Timestamp(ts_str)
            return f"{timestamp.year:04d}-{timestamp.month:02d}"
    
    def _format_seasonal_period(self, label) -> str:
        """Format seasonal period label."""
        if hasattr(label, 'year') and hasattr(label, 'month'):
            timestamp_year = label.year
            timestamp_month = label.month
        else:
            # Convert to string and parse
            ts_str = str(label)
            timestamp = pd.Timestamp(ts_str) 
            timestamp_year = timestamp.year
            timestamp_month = timestamp.month
            
        season_names = {12: 'DJF', 3: 'MAM', 6: 'JJA', 9: 'SON'}
        season = season_names.get(timestamp_month, f'S{(timestamp_month-1)//3 + 1}')
        return f'{timestamp_year}-{season}'
    
    def _format_yearly_period(self, label) -> str:
        """Format yearly period label."""
        if hasattr(label, 'year'):
            return f"{label.year:04d}"
        else:
            # Convert to string and parse
            ts_str = str(label)
            timestamp = pd.Timestamp(ts_str)
            return f"{timestamp.year:04d}"

    def _perform_rolling_averaging(self, dataset: xr.Dataset, output_dir: str, 
                                 window_config: Dict[str, Any]) -> List[str]:
        """Perform rolling average with single output file."""
        window_size = window_config['window_size']
        center = window_config.get('center', True)
        
        # Convert window_size to integer time steps for xarray rolling
        # First convert to pandas Timedelta, then figure out how many time steps
        window_timedelta = pd.Timedelta(window_size)
        
        # Calculate time step from dataset
        time_coord = dataset.coords['time']
        if len(time_coord) > 1:
            time_step = time_coord.isel(time=1) - time_coord.isel(time=0)
            time_step_pd = pd.Timedelta(time_step.values)
            
            # Calculate window size in time steps
            window_steps = int(window_timedelta / time_step_pd)
        else:
            # Default to reasonable window if only one time point
            window_steps = max(1, int(window_timedelta.total_seconds() / 86400))  # days
        
        # Split dataset into regular variables and timedelta variables
        timedelta_vars = {}
        regular_vars = {}
        
        for var_name in dataset.data_vars:
            var = dataset[var_name]
            if var.dtype.kind == 'm':  # timedelta
                timedelta_vars[var_name] = var
            else:
                regular_vars[var_name] = var
        
        # Create dataset with only regular variables for rolling operation
        regular_dataset = xr.Dataset(
            data_vars=regular_vars,
            coords=dataset.coords,
            attrs=dataset.attrs
        )
        
        # Perform rolling mean on regular variables
        rolling_avg = regular_dataset.rolling(time=window_steps, center=center).mean()
        
        # Handle timedelta variables separately (they should stay constant for MOM6)
        for var_name, var in timedelta_vars.items():
            # For timedelta variables like average_DT, just copy the values
            # since they represent model time intervals which don't change
            rolling_avg[var_name] = var
        
        # Drop NaN values that result from rolling operation
        rolling_avg = rolling_avg.dropna(dim='time')
        
        # Generate filename 
        window_clean = window_size.replace('D', 'day').replace('H', 'hour').replace('T', 'min')
        period_str = f"rolling_{window_clean}"
        if center:
            period_str += "_centered"
        filename = self._generate_output_filename('rolling', period_str)
        filepath = os.path.join(output_dir, filename)
        
        # Save to file with proper encoding for timedelta variables
        encoding = {}
        if 'average_DT' in rolling_avg.variables:
            # Encode timedelta as float64 in days
            encoding['average_DT'] = {
                'dtype': 'float64',
                'units': 'days'
            }
        
        rolling_avg.to_netcdf(filepath, encoding=encoding)
        
        return [filepath]

    def _perform_custom_averaging(self, dataset: xr.Dataset, output_dir: str,
                                window_config: Dict[str, Any]) -> List[str]:
        """Perform custom frequency resampling."""
        freq = window_config['freq']
        output_files = []
        
        # Group by custom frequency
        grouped = dataset.resample(time=freq)
        
        for label, group in grouped:
            if len(group.time) == 0:
                continue
                
            # Calculate average for this group
            avg = group.mean(dim='time', keep_attrs=True)
            
            # Update time coordinate - keep as array
            avg = avg.expand_dims('time')
            avg = avg.assign_coords(time=[label])
            
            # Generate filename based on the frequency and period
            if hasattr(label, 'strftime'):
                period_str = label.strftime('%Y-%m-%d')
            else:
                period_str = str(label).split('T')[0]  # Extract date part
                
            filename = self._generate_output_filename('custom', f"{freq}_{period_str}")
            filepath = os.path.join(output_dir, filename)
            
            # Save to file with proper encoding for timedelta variables
            encoding = {}
            if 'average_DT' in avg.variables:
                # Encode timedelta as float64 in days
                encoding['average_DT'] = {
                    'dtype': 'float64',
                    'units': 'days'
                }
            
            avg.to_netcdf(filepath, encoding=encoding)
            output_files.append(filepath)
            
        return output_files
        
    def time_average(self) -> List[str]:
        """Perform time averaging on input files.
        
        Returns:
            List of output file paths created
            
        Raises:
            FileNotFoundError: If input files not found
            ValueError: If configuration is invalid
        """
        # Get input files
        input_files = self._get_input_files()
        print(f"Found {len(input_files)} input files")
        
        # Create output directory
        self._ensure_output_directory()
        output_dir = self.config['output_directory']
        
        # Load dataset with chunking for memory efficiency
        print("Loading dataset...")
        variables = self.config.get('variables')
        
        # Open dataset with error handling for cftime
        try:
            dataset = xr.open_mfdataset(
                input_files, 
                combine='by_coords',
                use_cftime=True,  # Handle non-standard calendars
                data_vars=variables if variables else 'all'
            )
        except Exception as e:
            # Fallback without cftime if it fails
            warnings.warn(f"Failed to open with cftime, trying without: {e}")
            dataset = xr.open_mfdataset(
                input_files,
                combine='by_coords', 
                data_vars=variables if variables else 'all'
            )
        
        # Detect native interval
        print("Detecting native time interval...")
        native_interval = self._detect_native_interval(dataset)
        print(f"Native interval: {native_interval}")
        
        # Validate averaging window
        self._validate_averaging_window_vs_native(native_interval, self.config['averaging_window'])
        
        # Perform averaging
        print("Performing time averaging...")
        output_files = self._perform_averaging(dataset, output_dir)
        
        dataset.close()
        
        print(f"Created {len(output_files)} output files in {output_dir}")
        return output_files
"""MOM6 time-averaging utility for CrocoCamp.

This module provides configurable time-averaging (resampling and rolling mean) 
on MOM6 NetCDF output files using xarray and dask. Outputs are fully compatible
with CrocoCamp's WorkflowModelObs workflow.
"""

import glob
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import warnings

import dask.array as da
import numpy as np
import pandas as pd
import xarray as xr
import yaml


class MOM6TimeAverager:
    """Time-averaging utility for MOM6 NetCDF files.
    
    Performs configurable time-averaging operations on MOM6 model output files
    while preserving compatibility with CrocoCamp's WorkflowModelObs workflow.
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
        """Load configuration from YAML file.
        
        Args:
            config_path: Path to YAML configuration file
            
        Returns:
            Configuration dictionary
            
        Raises:
            FileNotFoundError: If config file doesn't exist
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
            
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
            
        return config
        
    def _validate_config(self) -> None:
        """Validate that required configuration keys are present.
        
        Raises:
            ValueError: If required keys are missing or invalid
        """
        required_keys = ['input_files_pattern', 'output_directory', 'averaging_window']
        missing_keys = [key for key in required_keys if key not in self.config]
        
        if missing_keys:
            raise ValueError(f"Missing required configuration keys: {missing_keys}")
            
        # Validate averaging window
        window = self.config['averaging_window']
        if isinstance(window, str):
            valid_windows = ['monthly', 'seasonal', 'yearly']
            if window not in valid_windows:
                raise ValueError(f"Invalid averaging_window: {window}. "
                               f"Must be one of {valid_windows} or a dict with 'type' key")
        elif isinstance(window, dict):
            if 'type' not in window:
                raise ValueError("averaging_window dict must contain 'type' key")
            
            window_type = window['type']
            if window_type == 'rolling':
                if 'window_size' not in window:
                    raise ValueError("rolling window must specify 'window_size'")
            elif window_type == 'custom':
                if 'freq' not in window:
                    raise ValueError("custom window must specify 'freq'")
        else:
            raise ValueError("averaging_window must be a string or dict")
    
    def _get_input_files(self) -> List[str]:
        """Get list of input files matching the glob pattern.
        
        Returns:
            Sorted list of file paths
            
        Raises:
            ValueError: If no files found matching pattern
        """
        pattern = self.config['input_files_pattern']
        files = glob.glob(pattern)
        
        if not files:
            raise ValueError(f"No files found matching pattern: {pattern}")
            
        return sorted(files)
    
    def _extract_native_time_interval(self, dataset: xr.Dataset) -> pd.Timedelta:
        """Extract native time interval from MOM6 dataset.
        
        Args:
            dataset: MOM6 dataset
            
        Returns:
            Native time interval as pandas Timedelta
            
        Raises:
            ValueError: If native interval cannot be determined
        """
        # Try to get from average_DT variable (MOM6 standard)
        if 'average_DT' in dataset.variables:
            dt_days = dataset['average_DT'].values
            if np.isscalar(dt_days):
                # Handle potential overflow issues
                dt_val = float(dt_days)
                if dt_val > 1e6:  # Likely in seconds instead of days
                    return pd.Timedelta(seconds=dt_val)
                else:
                    return pd.Timedelta(days=dt_val)
            else:
                # Use the first value if it's an array
                dt_val = float(dt_days.flat[0])
                if dt_val > 1e6:  # Likely in seconds instead of days  
                    return pd.Timedelta(seconds=dt_val)
                else:
                    return pd.Timedelta(days=dt_val)
        
        # Try to get from global attribute
        if hasattr(dataset, 'attrs') and 'dt_therm' in dataset.attrs:
            dt_seconds = dataset.attrs['dt_therm']
            return pd.Timedelta(seconds=float(dt_seconds))
            
        # Try to infer from time coordinate
        if 'time' in dataset.coords:
            time_coord = dataset.time
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
        
        # Default fallback - warn user
        warnings.warn("Could not determine native time interval from dataset. "
                     "Using default of 1 day. This may affect validation.",
                     UserWarning)
        return pd.Timedelta(days=1)
    
    def _validate_averaging_window(self, native_interval: pd.Timedelta, 
                                   requested_window: Union[str, Dict[str, Any]]) -> None:
        """Validate that averaging window is not shorter than native interval.
        
        Args:
            native_interval: Native time interval from model
            requested_window: Requested averaging window
            
        Raises:
            ValueError: If requested window is shorter than native interval
        """
        if isinstance(requested_window, str):
            # Convert common window strings to approximate timedeltas for comparison
            window_deltas = {
                'monthly': pd.Timedelta(days=30),  # Approximate
                'seasonal': pd.Timedelta(days=90),  # Approximate 
                'yearly': pd.Timedelta(days=365)   # Approximate
            }
            window_delta = window_deltas.get(requested_window)
            
            if window_delta and window_delta < native_interval:
                raise ValueError(f"Requested averaging window '{requested_window}' "
                               f"({window_delta}) is shorter than native model interval "
                               f"({native_interval})")
                               
        elif isinstance(requested_window, dict):
            window_type = requested_window['type']
            if window_type == 'rolling':
                # For rolling windows, check the window size
                window_size = requested_window['window_size']
                if isinstance(window_size, str):
                    try:
                        window_delta = pd.Timedelta(window_size)
                        if window_delta < native_interval:
                            raise ValueError(f"Rolling window size '{window_size}' "
                                           f"is shorter than native model interval "
                                           f"({native_interval})")
                    except ValueError as e:
                        raise ValueError(f"Invalid window_size format: {window_size}") from e
    
    def _generate_output_filename(self, avg_type: str, period_info: str) -> str:
        """Generate output filename based on averaging type and period.
        
        Args:
            avg_type: Type of averaging ('monthly', 'seasonal', 'rolling', etc.)
            period_info: Period-specific information for filename
            
        Returns:
            Generated filename
        """
        base_name = f"avg_{avg_type}_{period_info}.nc"
        return base_name
    
    def _perform_monthly_averaging(self, dataset: xr.Dataset, output_dir: str) -> List[str]:
        """Perform monthly averaging with one file per month.
        
        Args:
            dataset: Input dataset
            output_dir: Output directory
            
        Returns:
            List of created file paths
        """
        output_files = []
        
        # Group by month and average - use 'ME' instead of deprecated 'M'
        monthly_groups = dataset.resample(time='ME')
        
        for label, group in monthly_groups:
            if len(group.time) == 0:
                continue
                
            # Calculate monthly average
            monthly_avg = group.mean(dim='time', keep_attrs=True)
            
            # Update time coordinate to represent the month - keep as array
            monthly_avg = monthly_avg.expand_dims('time')
            monthly_avg = monthly_avg.assign_coords(time=[label])
            
            # Generate filename - handle cftime objects
            if hasattr(label, 'strftime'):
                period_str = label.strftime('%Y-%m')
            else:
                # Convert cftime to pandas timestamp for formatting
                period_str = pd.Timestamp(str(label)).strftime('%Y-%m')
            filename = self._generate_output_filename('month', period_str)
            filepath = os.path.join(output_dir, filename)
            
            # Save to file
            monthly_avg.to_netcdf(filepath)
            output_files.append(filepath)
            
        return output_files
    
    def _perform_seasonal_averaging(self, dataset: xr.Dataset, output_dir: str) -> List[str]:
        """Perform seasonal averaging with one file per season.
        
        Args:
            dataset: Input dataset
            output_dir: Output directory
            
        Returns:
            List of created file paths
        """
        output_files = []
        
        # Group by season (quarterly)
        seasonal_groups = dataset.resample(time='QS-DEC')  # Seasons starting in Dec
        
        for label, group in seasonal_groups:
            if len(group.time) == 0:
                continue
                
            # Calculate seasonal average
            seasonal_avg = group.mean(dim='time', keep_attrs=True)
            
            # Update time coordinate to represent the season - keep as array
            seasonal_avg = seasonal_avg.expand_dims('time')
            seasonal_avg = seasonal_avg.assign_coords(time=[label])
            
            # Generate filename with year and season - handle cftime objects
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
            period_str = f'{timestamp_year}-{season}'
            filename = self._generate_output_filename('season', period_str)
            filepath = os.path.join(output_dir, filename)
            
            # Save to file
            seasonal_avg.to_netcdf(filepath)
            output_files.append(filepath)
            
        return output_files
    
    def _perform_yearly_averaging(self, dataset: xr.Dataset, output_dir: str) -> List[str]:
        """Perform yearly averaging with one file per year.
        
        Args:
            dataset: Input dataset  
            output_dir: Output directory
            
        Returns:
            List of created file paths
        """
        output_files = []
        
        # Group by year
        yearly_groups = dataset.resample(time='YS')  # Year start
        
        for label, group in yearly_groups:
            if len(group.time) == 0:
                continue
                
            # Calculate yearly average
            yearly_avg = group.mean(dim='time', keep_attrs=True)
            
            # Update time coordinate to represent the year - keep as array
            yearly_avg = yearly_avg.expand_dims('time')
            yearly_avg = yearly_avg.assign_coords(time=[label])
            
            # Generate filename - handle cftime objects
            if hasattr(label, 'year'):
                period_str = str(label.year)
            else:
                period_str = pd.Timestamp(str(label)).strftime('%Y')
            filename = self._generate_output_filename('year', period_str)
            filepath = os.path.join(output_dir, filename)
            
            # Save to file
            yearly_avg.to_netcdf(filepath)
            output_files.append(filepath)
            
        return output_files
    
    def _perform_custom_averaging(self, dataset: xr.Dataset, output_dir: str, 
                                freq: str) -> List[str]:
        """Perform custom frequency averaging with one file per period.
        
        Args:
            dataset: Input dataset
            output_dir: Output directory
            freq: Pandas frequency string for resampling
            
        Returns:
            List of created file paths
        """
        output_files = []
        
        # Group by custom frequency
        custom_groups = dataset.resample(time=freq)
        
        for label, group in custom_groups:
            if len(group.time) == 0:
                continue
                
            # Calculate average
            custom_avg = group.mean(dim='time', keep_attrs=True)
            
            # Update time coordinate - keep as array
            custom_avg = custom_avg.expand_dims('time')
            custom_avg = custom_avg.assign_coords(time=[label])
            
            # Generate filename - handle cftime objects
            if hasattr(label, 'strftime'):
                period_str = label.strftime('%Y-%m-%d')
            else:
                period_str = pd.Timestamp(str(label)).strftime('%Y-%m-%d')
            filename = self._generate_output_filename('custom', period_str)
            filepath = os.path.join(output_dir, filename)
            
            # Save to file
            custom_avg.to_netcdf(filepath)
            output_files.append(filepath)
            
        return output_files
    
    def _perform_rolling_averaging(self, dataset: xr.Dataset, output_dir: str,
                                  window_size: Union[int, str], 
                                  center: bool = True) -> List[str]:
        """Perform rolling average with single output file.
        
        Args:
            dataset: Input dataset
            output_dir: Output directory
            window_size: Size of rolling window
            center: Whether to center the rolling window
            
        Returns:
            List containing path to single output file
        """
        # Determine window size for rolling operation
        if isinstance(window_size, str):
            # Convert time-based window to number of time steps
            window_timedelta = pd.Timedelta(window_size)
            native_interval = self._extract_native_time_interval(dataset)
            window_steps = max(1, int(window_timedelta / native_interval))
        else:
            window_steps = int(window_size)
        
        # Perform rolling average
        rolling_avg = dataset.rolling(time=window_steps, center=center).mean()
        
        # Generate filename
        if isinstance(window_size, str):
            window_str = window_size.replace(' ', '').replace('days', 'D')
        else:
            window_str = f'{window_size}steps'
            
        filename = self._generate_output_filename('rolling', window_str)
        filepath = os.path.join(output_dir, filename)
        
        # Save to file
        rolling_avg.to_netcdf(filepath)
        
        return [filepath]
    
    def run(self) -> List[str]:
        """Run the time averaging operation.
        
        Returns:
            List of created output file paths
            
        Raises:
            ValueError: If operation fails
        """
        print("Starting MOM6 time averaging operation...")
        
        # Get input files
        input_files = self._get_input_files()
        print(f"Found {len(input_files)} input files")
        
        # Create output directory
        output_dir = self.config['output_directory']
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Load dataset with dask for scalability
        print("Loading dataset with dask...")
        dataset = xr.open_mfdataset(
            input_files,
            chunks={'time': 10},  # Chunk along time dimension
            decode_times=True,
            use_cftime=True,
            decode_timedelta=False  # Avoid issues with average_DT decoding
        )
        
        # Filter variables if specified
        variables = self.config.get('variables')
        if variables:
            # Always keep coordinate variables
            coord_vars = list(dataset.coords.keys())
            keep_vars = list(variables) + coord_vars
            # Also keep time bounds and averaging info if present
            for var in ['time_bounds', 'average_T1', 'average_T2', 'average_DT']:
                if var in dataset.variables:
                    keep_vars.append(var)
            dataset = dataset[keep_vars]
        
        # Extract and validate native time interval
        native_interval = self._extract_native_time_interval(dataset)
        print(f"Detected native time interval: {native_interval}")
        
        # Get averaging window configuration
        averaging_window = self.config['averaging_window']
        self._validate_averaging_window(native_interval, averaging_window)
        
        # Perform averaging based on window type
        output_files = []
        
        if isinstance(averaging_window, str):
            if averaging_window == 'monthly':
                output_files = self._perform_monthly_averaging(dataset, output_dir)
            elif averaging_window == 'seasonal':
                output_files = self._perform_seasonal_averaging(dataset, output_dir)
            elif averaging_window == 'yearly':
                output_files = self._perform_yearly_averaging(dataset, output_dir)
        
        elif isinstance(averaging_window, dict):
            window_type = averaging_window['type']
            
            if window_type == 'rolling':
                window_size = averaging_window['window_size']
                center = averaging_window.get('center', True)
                output_files = self._perform_rolling_averaging(
                    dataset, output_dir, window_size, center
                )
            elif window_type == 'custom':
                freq = averaging_window['freq']
                output_files = self._perform_custom_averaging(dataset, output_dir, freq)
        
        print(f"Created {len(output_files)} output files:")
        for filepath in output_files:
            print(f"  - {filepath}")
            
        return output_files


def time_average_from_config(config_path: str) -> List[str]:
    """Convenience function to run time averaging from a config file.
    
    Args:
        config_path: Path to YAML configuration file
        
    Returns:
        List of created output file paths
    """
    averager = MOM6TimeAverager(config_path)
    return averager.run()
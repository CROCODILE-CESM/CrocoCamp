"""Tests for time averaging functionality."""

import os
import tempfile
from pathlib import Path
from typing import Dict, Any

import numpy as np
import pandas as pd
import pytest
import xarray as xr
import yaml

from crococamp.io.mom6_time_averager import MOM6TimeAverager, time_average_from_config


def create_mock_mom6_dataset(times: pd.DatetimeIndex, 
                            include_avg_dt: bool = True) -> xr.Dataset:
    """Create a mock MOM6 dataset for testing.
    
    Args:
        times: Time coordinate values
        include_avg_dt: Whether to include average_DT variable
        
    Returns:
        Mock MOM6 dataset with basic structure
    """
    # Create spatial dimensions matching MOM6 structure
    xh = np.linspace(-80, -60, 10)  # Longitude 
    yh = np.linspace(30, 45, 8)    # Latitude
    zl = np.array([5, 15, 25, 50])  # Depth levels
    
    # Create meshgrid for 3D variables
    time_len = len(times)
    nz, ny, nx = len(zl), len(yh), len(xh)
    
    # Create mock data variables following MOM6 naming conventions
    dataset = xr.Dataset(
        {
            # Temperature - key MOM6 variable
            'thetao': (('time', 'zl', 'yh', 'xh'), 
                      np.random.randn(time_len, nz, ny, nx) + 15.0),
            
            # Salinity - another key variable  
            'so': (('time', 'zl', 'yh', 'xh'),
                   np.random.randn(time_len, nz, ny, nx) + 35.0),
            
            # Sea surface height
            'SSH': (('time', 'yh', 'xh'),
                    np.random.randn(time_len, ny, nx) * 0.1),
                    
            # Surface temperature
            'tos': (('time', 'yh', 'xh'),
                    np.random.randn(time_len, ny, nx) + 18.0),
        },
        coords={
            'time': times,
            'xh': xh,
            'yh': yh, 
            'zl': zl,
        }
    )
    
    # Add MOM6-style attributes
    dataset.attrs.update({
        'title': 'Mock MOM6 dataset for testing',
        'grid_type': 'regular'
    })
    
    # Add variable attributes similar to MOM6
    dataset['thetao'].attrs.update({
        'units': 'degC',
        'long_name': 'Sea Water Potential Temperature',
        'standard_name': 'sea_water_potential_temperature',
        'time_avg_info': 'average_T1,average_T2,average_DT'
    })
    
    dataset['so'].attrs.update({
        'units': 'psu',
        'long_name': 'Sea Water Salinity', 
        'standard_name': 'sea_water_salinity',
        'time_avg_info': 'average_T1,average_T2,average_DT'
    })
    
    # Add time-related variables if requested
    if include_avg_dt:
        # Add average_DT variable (time interval in days)
        dataset['average_DT'] = (('time',), np.full(time_len, 1.0))  # 1 day intervals
        dataset['average_DT'].attrs.update({
            'units': 'days',
            'long_name': 'Length of average period'
        })
        
        # Add time bounds
        nbnd = 2
        time_bounds = np.zeros((time_len, nbnd))
        for i, t in enumerate(times):
            # Use simple day numbers since epoch
            day_start = i  
            day_end = i + 1
            time_bounds[i, 0] = day_start
            time_bounds[i, 1] = day_end
            
        dataset['time_bounds'] = (('time', 'nbnd'), time_bounds)
        dataset.coords['nbnd'] = np.arange(nbnd)
    
    return dataset


def create_test_config(input_pattern: str, output_dir: str, 
                      averaging_window: Any) -> Dict[str, Any]:
    """Create test configuration dictionary.
    
    Args:
        input_pattern: Glob pattern for input files
        output_dir: Output directory path
        averaging_window: Averaging window specification
        
    Returns:
        Configuration dictionary
    """
    return {
        'input_files_pattern': input_pattern,
        'output_directory': output_dir,
        'averaging_window': averaging_window,
        'variables': ['thetao', 'so', 'SSH']  # Subset of variables to speed up tests
    }


def test_monthly_averaging():
    """Test monthly averaging functionality."""
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock data with daily data for 3 months
        times = pd.date_range('2010-01-01', '2010-03-31', freq='D')
        dataset = create_mock_mom6_dataset(times)
        
        # Save test input file
        input_file = os.path.join(temp_dir, 'test_input.nc')
        dataset.to_netcdf(input_file)
        
        # Create output directory
        output_dir = os.path.join(temp_dir, 'output')
        os.makedirs(output_dir)
        
        # Create config with new format
        config = create_test_config(input_file, output_dir, {
            'type': 'predefined',
            'window_size': 'monthly'
        })
        config_file = os.path.join(temp_dir, 'config.yaml')
        
        with open(config_file, 'w') as f:
            yaml.dump(config, f)
        
        # Run averaging
        averager = MOM6TimeAverager(config_file)
        output_files = averager.time_average()
        
        # Should create 3 monthly files
        assert len(output_files) == 3
        
        # Check filenames - new format includes original file base name
        actual_filenames = [os.path.basename(f) for f in output_files]
        
        # All files should have the right pattern with month and time info
        for filename in actual_filenames:
            assert '_month_' in filename
            assert filename.endswith('.nc')
            assert '2010-' in filename  # Should have year-month pattern
            
        # Check that we have files for each month
        month_patterns = ['2010-01', '2010-02', '2010-03']
        for pattern in month_patterns:
            assert any(pattern in f for f in actual_filenames), f"No file found for {pattern}"
        
        # Check that files exist and have correct structure
        for output_file in output_files:
            assert os.path.exists(output_file)
            
            # Load and check structure
            with xr.open_dataset(output_file) as ds:
                assert 'thetao' in ds.variables
                assert 'so' in ds.variables 
                assert 'SSH' in ds.variables
                assert len(ds.time) == 1  # One time point per monthly average


def test_rolling_averaging():
    """Test rolling average functionality."""
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock data with daily data for 30 days
        times = pd.date_range('2010-01-01', '2010-01-30', freq='D')
        dataset = create_mock_mom6_dataset(times)
        
        # Save test input file
        input_file = os.path.join(temp_dir, 'test_input.nc')
        dataset.to_netcdf(input_file)
        
        # Create output directory
        output_dir = os.path.join(temp_dir, 'output')
        os.makedirs(output_dir)
        
        # Create rolling window config (7-day rolling average)
        rolling_config = {
            'type': 'rolling',
            'window_size': '7D',
            'center': True
        }
        config = create_test_config(input_file, output_dir, rolling_config)
        config_file = os.path.join(temp_dir, 'config.yaml')
        
        with open(config_file, 'w') as f:
            yaml.dump(config, f)
        
        # Run averaging
        averager = MOM6TimeAverager(config_file)
        output_files = averager.time_average()
        
        # Should create 1 file for rolling average
        assert len(output_files) == 1
        
        # Check filename - more flexible pattern matching
        output_file = output_files[0]
        filename = os.path.basename(output_file)
        assert 'rolling' in filename
        assert '7' in filename  # Should have the window size
        assert 'centered' in filename  # Should indicate centered rolling
        assert filename.endswith('.nc')
        
        # Check file structure
        assert os.path.exists(output_file)
        with xr.open_dataset(output_file) as ds:
            assert 'thetao' in ds.variables
            # Rolling average with centered window reduces time points due to edge effects
            # For 30 input days with 7-day centered window, expect ~24 points
            assert len(ds.time) < len(times)  # Should be fewer than original
            assert len(ds.time) >= 20  # Should have substantial data remaining


def test_config_validation():
    """Test configuration validation."""
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Test missing required keys
        incomplete_config = {'output_directory': temp_dir}
        config_file = os.path.join(temp_dir, 'config.yaml')
        
        with open(config_file, 'w') as f:
            yaml.dump(incomplete_config, f)
        
        with pytest.raises(ValueError, match="Required configuration keys missing"):
            MOM6TimeAverager(config_file)
        
        # Create a dummy .nc file for file pattern validation
        dummy_file = os.path.join(temp_dir, 'dummy.nc')
        with open(dummy_file, 'wb') as f:
            f.write(b'dummy')
        
        # Test invalid averaging window
        invalid_config = {
            'input_files_pattern': os.path.join(temp_dir, '*.nc'),
            'output_directory': temp_dir,
            'averaging_window': 'invalid_window'
        }
        
        with open(config_file, 'w') as f:
            yaml.dump(invalid_config, f)
        
        with pytest.raises(ValueError, match="Invalid averaging window string"):
            MOM6TimeAverager(config_file)


def test_native_interval_extraction():
    """Test extraction of native time interval from dataset."""
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create dataset with average_DT - use smaller value
        times = pd.date_range('2010-01-01', '2010-01-05', freq='D')
        dataset = create_mock_mom6_dataset(times, include_avg_dt=False)
        
        # Manually add average_DT with a reasonable value (1 day in days)
        dataset['average_DT'] = (('time',), np.full(len(times), 1.0))
        dataset['average_DT'].attrs.update({
            'units': 'days',
            'long_name': 'Length of average period'
        })
        
        input_file = os.path.join(temp_dir, 'test_input.nc')
        dataset.to_netcdf(input_file)
        
        output_dir = os.path.join(temp_dir, 'output')
        config = create_test_config(input_file, output_dir, {
            'type': 'predefined', 
            'window_size': 'monthly'
        })
        config_file = os.path.join(temp_dir, 'config.yaml')
        
        with open(config_file, 'w') as f:
            yaml.dump(config, f)
        
        averager = MOM6TimeAverager(config_file)
        
        # Load dataset to test interval extraction  
        with xr.open_dataset(input_file, decode_timedelta=False) as ds:
            interval = averager._detect_native_interval(ds)
            assert interval == pd.Timedelta(days=1)


def test_workflow_compatibility():
    """Test that output files are compatible with WorkflowModelObs workflow."""
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock data
        times = pd.date_range('2010-01-01', '2010-01-31', freq='D')
        dataset = create_mock_mom6_dataset(times)
        
        input_file = os.path.join(temp_dir, 'test_input.nc')
        dataset.to_netcdf(input_file)
        
        output_dir = os.path.join(temp_dir, 'output')
        config = create_test_config(input_file, output_dir, {
            'type': 'predefined',
            'window_size': 'monthly'
        })
        config_file = os.path.join(temp_dir, 'config.yaml')
        
        with open(config_file, 'w') as f:
            yaml.dump(config, f)
        
        # Run averaging
        output_files = time_average_from_config(config_file)
        
        # Check that outputs can be read as valid MOM6 files
        for output_file in output_files:
            with xr.open_dataset(output_file) as ds:
                # Check for essential coordinates and dimensions
                assert 'time' in ds.coords
                assert 'xh' in ds.coords  
                assert 'yh' in ds.coords
                assert 'zl' in ds.coords
                
                # Check that variables maintain their attributes
                assert ds['thetao'].attrs.get('units') == 'degC'
                assert ds['so'].attrs.get('units') == 'psu'
                
                # Check that time coordinate is properly set
                assert len(ds.time) == 1  # Monthly averages have single time point
                
                # Variables should have expected dimensions  
                assert ds['thetao'].dims == ('time', 'zl', 'yh', 'xh')
                assert ds['SSH'].dims == ('time', 'yh', 'xh')


if __name__ == '__main__':
    # Run tests manually if called directly
    test_monthly_averaging()
    test_rolling_averaging() 
    test_config_validation()
    test_native_interval_extraction()
    test_workflow_compatibility()
    print("All tests passed!")
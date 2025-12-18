"""Unit tests for ModelAdapter class and subclasses."""

import os
import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import patch, mock_open
import xarray as xr

from crococamp.model_adapter.model_adapter import ModelAdapter
from crococamp.model_adapter.model_adapter_MOM6 import ModelAdapterMOM6
from crococamp.model_adapter.model_adapter_ROMS import ModelAdapterROMS
from crococamp.model_adapter.registry import create_model_adapter

@pytest.fixture
def create_tmp_MOM6_nc(tmp_path):
    """Create temporary netcdf file to read from disk"""

    original_dir = os.getcwd()
    os.chdir(tmp_path)
    # Create minimal NetCDF files using xarray
    ds_template = xr.Dataset(
        data_vars={"temp": (["time", "lat", "lon"], np.zeros((1, 10, 10)))},
        coords={"time": [0], "lat": np.arange(10), "lon": np.arange(10)}
    )
    model_nc = tmp_path / "model.nc"
    ds_template.to_netcdf(model_nc)

@pytest.fixture
def create_tmp_ROMS_nc(tmp_path):
    """Create temporary netcdf file to read from disk"""

    original_dir = os.getcwd()
    os.chdir(tmp_path)
    # Create minimal NetCDF files using xarray
    ds_template = xr.Dataset(
        data_vars={"temp": (["ocean_time", "lat", "lon"], np.zeros((1, 10, 10)))},
        coords={"ocean_time": [0], "lat": np.arange(10), "lon": np.arange(10)}
    )
    model_nc = tmp_path / "model.nc"
    ds_template.to_netcdf(model_nc)
    
class TestModelAdapterMOM6:
    """Test ModelAdapterMOM6 methods"""

    def test_init(self):
        """Test constructor"""

        model_adapter = create_model_adapter("mom6")

        assert isinstance(model_adapter.time_varname, str)
        assert model_adapter.time_varname == "time"

    def test_get_required_config_keys(self):
        """Test get_required_config_keys returns complete list."""

        target_keys = [
            'model_files_folder', 
            'obs_seq_in_folder', 
            'output_folder',
            'template_file', 
            'static_file', 
            'ocean_geometry',
            'perfect_model_obs_dir', 
            'parquet_folder'
        ]

        model_adapter = create_model_adapter("mom6")
        required_keys = model_adapter.get_required_config_keys()
        
        assert isinstance(required_keys, list)
        assert all(isinstance(item, str) for item in required_keys)
        assert required_keys == target_keys

    def test_get_common_model_keys(self):
        """Test get_required_config_keys returns complete list."""

        target_keys = [
            'model_files_folder', 
            'obs_seq_in_folder', 
            'output_folder',
            'roms_filename',
            'perfect_model_obs_dir', 
            'parquet_folder'
        ]

        model_adapter = create_model_adapter("mom6")
        common_model_keys = model_adapter.get_common_model_keys()
        
        assert isinstance(common_model_keys, list)
        assert all(isinstance(item, str) for item in common_model_keys)
        assert common_model_keys == target_keys

    def test_open_dataset_ctx(self, create_tmp_MOM6_nc, tmp_path):
        """Test open_dataset_ctx updates calendar and time varname"""
        model_adapter = create_model_adapter("mom6")

        model_nc = tmp_path / "model.nc"
        with model_adapter.open_dataset_ctx(model_nc) as ds:
            assert "time" in ds.coords
            assert ds[model_adapter.time_varname].attrs.get("calendar") == "proleptic_gregorian"


class TestModelAdapterROMS:
    """Test ModelAdapterROMS methods"""

    def test_init(self):
        """Test constructor"""

        model_adapter = create_model_adapter("roms")

        assert isinstance(model_adapter.time_varname, str)
        assert model_adapter.time_varname == "ocean_time"

    def test_get_required_config_keys(self):
        """Test get_required_config_keys returns complete list."""

        target_keys = [
            'template_file',
            'static_file',
            'ocean_geometry',
            'model_state_variables',
            'layer_name'
        ]

        model_adapter = create_model_adapter("roms")
        required_keys = model_adapter.get_required_config_keys()
        
        assert isinstance(required_keys, list)
        assert all(isinstance(item, str) for item in required_keys)
        assert required_keys == target_keys

    def test_get_common_model_keys(self):
        """Test get_required_config_keys returns complete list."""

        target_keys = [
            'roms_filename',
            'variables',
            'debug'
        ]

        model_adapter = create_model_adapter("roms")
        common_model_keys = model_adapter.get_common_model_keys()
        
        assert isinstance(common_model_keys, list)
        assert all(isinstance(item, str) for item in common_model_keys)
        assert common_model_keys == target_keys

    def test_open_dataset_ctx(self, create_tmp_ROMS_nc, tmp_path):
        """Test open_dataset_ctx updates calendar and time varname"""
        model_adapter = create_model_adapter("roms")

        model_nc = tmp_path / "model.nc"
        with model_adapter.open_dataset_ctx(model_nc) as ds:
            assert "time" in ds.coords

    def test_convert_units(self):
        """Test convert_units updates salinity values"""

        obs_types = [
            "BOTTLE_SALINITY",
            "ARGO_SALINITY",
            "SALINITY",
            "TEMPERATURE",
            "ARGO_TEMPERATURE",
            "SALTY"
        ]

        obs_values = np.array([30*1e3, 33*1e3, 35.1*1e3, 14, 16.7, 49])
        interpolated = obs_values + 0.023
        mock_df = pd.DataFrame({
            'interpolated_model': interpolated,
            'obs': obs_values,
            'type': obs_types
        })

        target_values = np.array([30, 33, 35.1, 14, 16.7, 49])

        target_df = pd.DataFrame({
            'interpolated_model': interpolated,
            'obs': target_values,
            'type': obs_types
        })
 
        
        model_adapter = create_model_adapter("roms")
        df = model_adapter.convert_units(mock_df)

        assert_frame_equal(mock_df, df)

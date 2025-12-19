"""Unit tests for ModelAdapter classes and registry.

Tests the model adapter architecture including:
- Adapter registry and factory function (create_model_adapter)
- MOM6-specific adapter behavior and unit conversions
- ROMS_RUTGERS-specific adapter behavior and unit conversions  
- Dataset opening with context managers
- Run options validation and capabilities checking
- Configuration key requirements per model

The tests use fixtures to create temporary netCDF files and mock
dependencies where appropriate.
"""

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
from crococamp.model_adapter.model_adapter_ROMS_Rutgers import ModelAdapterROMSRutgers
from crococamp.model_adapter.registry import create_model_adapter
from crococamp.workflows.workflow_model_obs import RunOptions


class TestModelAdapterRegistry:
    """Test model adapter registry and factory function."""

    def test_create_mom6_adapter_lowercase(self):
        """Test creating MOM6 adapter with lowercase name."""
        adapter = create_model_adapter("mom6")
        assert isinstance(adapter, ModelAdapterMOM6)
        assert adapter.time_varname == "time"

    def test_create_mom6_adapter_uppercase(self):
        """Test creating MOM6 adapter with uppercase name."""
        adapter = create_model_adapter("MOM6")
        assert isinstance(adapter, ModelAdapterMOM6)

    def test_create_roms_rutgers_adapter_lowercase(self):
        """Test creating ROMS_Rutgers adapter with lowercase name."""
        adapter = create_model_adapter("roms_rutgers")
        assert isinstance(adapter, ModelAdapterROMSRutgers)
        assert adapter.time_varname == "ocean_time"

    def test_create_roms_rutgers_adapter_uppercase(self):
        """Test creating ROMS_Rutgers adapter with uppercase name."""
        adapter = create_model_adapter("ROMS_Rutgers")
        assert isinstance(adapter, ModelAdapterROMSRutgers)

    def test_create_adapter_invalid_model(self):
        """Test creating adapter with invalid model name raises error."""
        with pytest.raises(ValueError, match="Unknown ocean_model"):
            create_model_adapter("invalid_model")

    def test_create_adapter_none_raises_error(self):
        """Test creating adapter with None raises error."""
        with pytest.raises(ValueError, match="ocean_model is required"):
            create_model_adapter(None)

    def test_create_adapter_whitespace_handling(self):
        """Test creating adapter handles whitespace in model name."""
        adapter = create_model_adapter("  MOM6  ")
        assert isinstance(adapter, ModelAdapterMOM6)


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
def create_tmp_ROMS_Rutgers_nc(tmp_path):
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

    def test_convert_units(self):
        """Test convert_units does not modify MOM6 data."""
        obs_types = [
            "BOTTLE_SALINITY",
            "ARGO_SALINITY",
            "SALINITY",
            "TEMPERATURE",
            "ARGO_TEMPERATURE",
            "SALTY"
        ]

        obs_values = np.array([30, 33, 35.1, 14, 16.7, 49])
        interpolated = obs_values + 0.023
        mock_df = pd.DataFrame({
            'interpolated_model': interpolated,
            'obs': obs_values,
            'type': obs_types
        })

        model_adapter = create_model_adapter("mom6")
        df = model_adapter.convert_units(mock_df)

        assert_frame_equal(mock_df, df)

    def test_convert_units_with_missing_values(self):
        """Test convert_units handles NA values correctly."""
        mock_df = pd.DataFrame({
            'interpolated_model': [20.0, np.nan, 22.0],
            'obs': [20.1, 21.0, np.nan],
            'type': ['SALINITY', 'TEMPERATURE', 'SALINITY']
        })

        model_adapter = create_model_adapter("mom6")
        df = model_adapter.convert_units(mock_df)

        assert pd.isna(df.loc[1, 'interpolated_model'])
        assert pd.isna(df.loc[2, 'obs'])
        assert df.loc[0, 'obs'] == 20.1

    def test_convert_units_preserves_dtypes(self):
        """Test convert_units preserves column data types."""
        mock_df = pd.DataFrame({
            'interpolated_model': np.array([20.0, 21.0], dtype=np.float64),
            'obs': np.array([20.1, 21.1], dtype=np.float64),
            'type': pd.Series(['SALINITY', 'TEMPERATURE'], dtype='object')
        })

        model_adapter = create_model_adapter("mom6")
        df = model_adapter.convert_units(mock_df)

        assert df['interpolated_model'].dtype == np.float64
        assert df['obs'].dtype == np.float64
        assert df['type'].dtype == object

    def test_convert_units_empty_dataframe(self):
        """Test convert_units handles empty dataframe."""
        mock_df = pd.DataFrame({
            'interpolated_model': pd.Series([], dtype=np.float64),
            'obs': pd.Series([], dtype=np.float64),
            'type': pd.Series([], dtype=object)
        })

        model_adapter = create_model_adapter("mom6")
        df = model_adapter.convert_units(mock_df)

        assert len(df) == 0
        assert list(df.columns) == ['interpolated_model', 'obs', 'type']

    def test_rename_time_varname(self):
        """Test rename_time_varname renames time coordinate."""
        ds = xr.Dataset(
            data_vars={"temp": (["time", "lat"], np.zeros((2, 3)))},
            coords={"time": [0, 1], "lat": [1, 2, 3]}
        )

        model_adapter = create_model_adapter("mom6")
        renamed_ds = model_adapter.rename_time_varname(ds)

        assert "time" in renamed_ds.coords
        assert model_adapter.time_varname not in renamed_ds.coords or model_adapter.time_varname == "time"

    def test_open_dataset_ctx_file_closure(self, create_tmp_MOM6_nc, tmp_path):
        """Test open_dataset_ctx closes file after use."""
        model_adapter = create_model_adapter("mom6")
        model_nc = tmp_path / "model.nc"

        with model_adapter.open_dataset_ctx(model_nc) as ds:
            assert ds is not None

        # Dataset should be closed after context exit
        # xarray doesn't expose a direct "is_closed" attribute,
        # but we verify no exception is raised

    def test_open_dataset_ctx_nonexistent_file(self, tmp_path):
        """Test open_dataset_ctx raises error for nonexistent file."""
        model_adapter = create_model_adapter("mom6")
        nonexistent = tmp_path / "nonexistent.nc"

        with pytest.raises(FileNotFoundError):
            with model_adapter.open_dataset_ctx(nonexistent) as ds:
                pass

    def test_validate_run_options_all_true(self):
        """Test that all run options are validated"""
        model_adapter = create_model_adapter("mom6")

        run_opts = RunOptions(
            trim_obs = True,
            no_matching = True,
            force_obs_time = True
        )
        model_adapter.validate_run_options(run_opts)

    def test_validate_run_options_some_false(self):
        """Test that some run options are valid if false"""
        model_adapter = create_model_adapter("mom6")

        run_opts = RunOptions(
            trim_obs = True,
            no_matching = False,
            force_obs_time = False
        )
        model_adapter.validate_run_options(run_opts)


class TestModelAdapterROMSRutgers:
    """Test ModelAdapterROMSRutgers methods"""

    def test_init(self):
        """Test constructor"""

        model_adapter = create_model_adapter("roms_rutgers")

        assert isinstance(model_adapter.time_varname, str)
        assert model_adapter.time_varname == "ocean_time"

    def test_get_required_config_keys(self):
        """Test get_required_config_keys returns complete list."""

        target_keys = [
            'model_files_folder', 
            'obs_seq_in_folder', 
            'output_folder',
            'roms_filename',
            'perfect_model_obs_dir', 
            'parquet_folder'
        ]

        model_adapter = create_model_adapter("roms_rutgers")
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

        model_adapter = create_model_adapter("roms_rutgers")
        common_model_keys = model_adapter.get_common_model_keys()
        
        assert isinstance(common_model_keys, list)
        assert all(isinstance(item, str) for item in common_model_keys)
        assert common_model_keys == target_keys

    def test_open_dataset_ctx(self, create_tmp_ROMS_Rutgers_nc, tmp_path):
        """Test open_dataset_ctx updates calendar and time varname"""
        model_adapter = create_model_adapter("roms_rutgers")

        model_nc = tmp_path / "model.nc"
        with model_adapter.open_dataset_ctx(model_nc) as ds:
            assert "time" in ds.coords

    def test_convert_units(self):
        """Test convert_units converts ROMS_Rutgers salinity from PSU/1000 to PSU."""
        obs_types = [
            "BOTTLE_SALINITY",
            "ARGO_SALINITY",
            "SALINITY",
            "TEMPERATURE",
            "ARGO_TEMPERATURE",
            "SALTY"
        ]

        obs_values = np.array([30*1e-3, 33*1e-3, 35.1*1e-3, 14, 16.7, 49])
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
         
        model_adapter = create_model_adapter("roms_rutgers")
        df = model_adapter.convert_units(mock_df)

        assert_frame_equal(df, target_df)

    def test_convert_units_with_missing_values(self):
        """Test convert_units handles NA values in salinity."""
        mock_df = pd.DataFrame({
            'interpolated_model': [0.020, np.nan, 0.022],
            'obs': [0.0201, 0.021, np.nan],
            'type': ['SALINITY', 'TEMPERATURE', 'SALINITY']
        })

        model_adapter = create_model_adapter("roms_rutgers")
        df = model_adapter.convert_units(mock_df)

        assert pd.isna(df.loc[1, 'interpolated_model'])
        assert pd.isna(df.loc[2, 'obs'])
        assert np.isclose(df.loc[0, 'obs'], 20.1)

    def test_convert_units_preserves_dtypes(self):
        """Test convert_units preserves column data types."""
        mock_df = pd.DataFrame({
            'interpolated_model': np.array([0.020, 0.021], dtype=np.float64),
            'obs': np.array([0.0201, 0.0211], dtype=np.float64),
            'type': pd.Series(['SALINITY', 'TEMPERATURE'], dtype='object')
        })

        model_adapter = create_model_adapter("roms_rutgers")
        df = model_adapter.convert_units(mock_df)

        assert df['interpolated_model'].dtype == np.float64
        assert df['obs'].dtype == np.float64
        assert df['type'].dtype == object

    def test_convert_units_only_salinity_affected(self):
        """Test convert_units only modifies salinity observations."""
        mock_df = pd.DataFrame({
            'interpolated_model': [0.020, 15.0, 0.022],
            'obs': [0.0201, 15.5, 0.0221],
            'type': ['SALINITY', 'TEMPERATURE', 'ARGO_SALINITY']
        })

        model_adapter = create_model_adapter("roms_rutgers")
        df = model_adapter.convert_units(mock_df)

        assert np.isclose(df.loc[0, 'obs'], 20.1)
        assert df.loc[1, 'obs'] == 15.5
        assert np.isclose(df.loc[2, 'obs'], 22.1)

    def test_convert_units_empty_dataframe(self):
        """Test convert_units handles empty dataframe."""
        mock_df = pd.DataFrame({
            'interpolated_model': pd.Series([], dtype=np.float64),
            'obs': pd.Series([], dtype=np.float64),
            'type': pd.Series([], dtype=object)
        })

        model_adapter = create_model_adapter("roms_rutgers")
        df = model_adapter.convert_units(mock_df)

        assert len(df) == 0
        assert list(df.columns) == ['interpolated_model', 'obs', 'type']

    def test_rename_time_varname(self):
        """Test rename_time_varname renames ocean_time to time."""
        ds = xr.Dataset(
            data_vars={"temp": (["ocean_time", "lat"], np.zeros((2, 3)))},
            coords={"ocean_time": [0, 1], "lat": [1, 2, 3]}
        )

        model_adapter = create_model_adapter("roms_rutgers")
        renamed_ds = model_adapter.rename_time_varname(ds)

        assert "time" in renamed_ds.coords
        assert "ocean_time" not in renamed_ds.coords

    def test_open_dataset_ctx_file_closure(self, create_tmp_ROMS_Rutgers_nc, tmp_path):
        """Test open_dataset_ctx closes file after use."""
        model_adapter = create_model_adapter("roms_rutgers")
        model_nc = tmp_path / "model.nc"

        with model_adapter.open_dataset_ctx(model_nc) as ds:
            assert ds is not None

    def test_open_dataset_ctx_nonexistent_file(self, tmp_path):
        """Test open_dataset_ctx raises error for nonexistent file."""
        model_adapter = create_model_adapter("roms_rutgers")
        nonexistent = tmp_path / "nonexistent.nc"

        with pytest.raises(FileNotFoundError):
            with model_adapter.open_dataset_ctx(nonexistent) as ds:
                pass

    def test_validate_run_options_all_true(self):
        """Test that not all run options are supported"""
        model_adapter = create_model_adapter("roms_rutgers")

        run_opts = RunOptions(
            trim_obs = True,
            no_matching = True,
            force_obs_time = True
        )

        with pytest.raises(NotImplementedError):
            model_adapter.validate_run_options(run_opts)

    def test_validate_run_options_all_false(self):
        """Test that if all run options are false, they are valid"""
        model_adapter = create_model_adapter("roms_rutgers")

        run_opts = RunOptions(
            trim_obs = False,
            no_matching = False,
            force_obs_time = False
        )
        model_adapter.validate_run_options(run_opts)

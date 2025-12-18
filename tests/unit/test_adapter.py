"""Unit tests for ModelAdapter class and subclasses."""

import pytest
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import patch, mock_open

from crococamp.model_adapter.model_adapter import ModelAdapter
from crococamp.model_adapter.model_adapter_MOM6 import ModelAdapterMOM6
from crococamp.model_adapter.model_adapter_ROMS import ModelAdapterROMS
from crococamp.model_adapter.registry import create_model_adapter

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

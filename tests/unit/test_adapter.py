"""Unit tests for ModelAdapter class and subclasses."""

import pytest
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import patch, mock_open

from crococamp.model_adapter.model_adapter import ModelAdapter
from crococamp.model_adapter.model_adapter_MOM6 import ModelAdapterMOM6
from crococamp.model_adapter.registry import create_model_adapter

class TestModelAdapterMOM6:
    """Test ModelAdapterMOM6 methods"""

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

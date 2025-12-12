"""Unit tests for base Workflow class.

Tests the abstract base Workflow class functionality including configuration
loading, validation, and workflow lifecycle management.
"""

import pytest
from typing import Any, Dict, List
from unittest.mock import patch, mock_open

from crococamp.workflows.workflow import Workflow


class ConcreteWorkflow(Workflow):
    """Concrete implementation of Workflow for testing."""
    
    def __init__(self, config: Dict[str, Any], required_keys: List[str] = None) -> None:
        """Initialize with optional required_keys override."""
        self._required_keys = required_keys if required_keys is not None else ['key1', 'key2']
        super().__init__(config)
    
    def get_required_config_keys(self) -> List[str]:
        """Return required configuration keys."""
        return self._required_keys
    
    def run(self) -> Any:
        """Execute the workflow."""
        return "workflow_executed"


class TestWorkflowInit:
    """Test Workflow initialization."""
    
    def test_init_with_valid_config(self):
        """Test initialization with valid configuration."""
        config = {'key1': 'value1', 'key2': 'value2', 'key3': 'value3'}
        workflow = ConcreteWorkflow(config)
        
        assert workflow.config == config
        assert workflow.get_config('key1') == 'value1'
        assert workflow.get_config('key2') == 'value2'
    
    def test_init_validates_config(self):
        """Test that initialization validates required keys."""
        config = {'key1': 'value1'}
        
        with pytest.raises(KeyError, match="Required keys missing from config"):
            ConcreteWorkflow(config)
    
    def test_init_with_minimal_config(self):
        """Test initialization with minimal required configuration."""
        config = {'key1': 'value1', 'key2': 'value2'}
        workflow = ConcreteWorkflow(config)
        
        assert workflow.config == config
    
    def test_init_with_empty_required_keys(self):
        """Test initialization when no keys are required."""
        config = {}
        workflow = ConcreteWorkflow(config, required_keys=[])
        
        assert workflow.config == {}


class TestWorkflowFromConfigFile:
    """Test Workflow.from_config_file class method."""
    
    def test_from_config_file_valid(self, tmp_path):
        """Test loading workflow from valid YAML file."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
key1: value1
key2: value2
key3: value3
time_window:
  days: 999
  hours: 0 
""")
        
        workflow = ConcreteWorkflow.from_config_file(str(config_file))
        
        assert workflow.get_config('key1') == str(tmp_path)+'/value1'
        assert workflow.get_config('key2') == str(tmp_path)+'/value2'
        assert workflow.get_config('key3') == str(tmp_path)+'/value3'
    
    def test_from_config_file_no_time_window(self, tmp_path):
        """Test loading workflow from valid YAML file."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
key1: value1
key2: value2
key3: value3
""")
        
        with pytest.raises(KeyError):
            ConcreteWorkflow.from_config_file(str(config_file))
    
    def test_from_config_file_with_kwargs_override(self, tmp_path):
        """Test that kwargs override config file values."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
key1: value1
key2: value2
time_window:
  days: 999
  hours: 0 
""")
        
        workflow = ConcreteWorkflow.from_config_file(
            str(config_file),
            key2='overridden'
        )
        
        assert workflow.get_config('key1') == str(tmp_path)+'/value1'
        assert workflow.get_config('key2') == 'overridden'
    
    def test_from_config_file_with_new_kwargs(self, tmp_path):
        """Test that kwargs can add new configuration keys."""
        config_file = tmp_path / "config.yaml"
        config_file.write_text("""
key1: value1
key2: value2
time_window:
  days: 999
  hours: 0 
""")
        
        workflow = ConcreteWorkflow.from_config_file(
            str(config_file),
            key3='new_value'
        )
        
        assert workflow.get_config('key1') == str(tmp_path)+'/value1'
        assert workflow.get_config('key2') == str(tmp_path)+'/value2'
        assert workflow.get_config('key3') == 'new_value'
    
    def test_from_config_file_missing(self):
        """Test error when config file does not exist."""
        with pytest.raises(FileNotFoundError):
            ConcreteWorkflow.from_config_file('/nonexistent/config.yaml')
    
    def test_from_config_file_invalid_yaml(self, tmp_path):
        """Test error with invalid YAML syntax."""
        config_file = tmp_path / "bad_config.yaml"
        config_file.write_text("""
key1: value1
key2: [unclosed list
""")
        
        with pytest.raises(Exception):
            ConcreteWorkflow.from_config_file(str(config_file))


class TestWorkflowConfigMethods:
    """Test Workflow configuration accessor methods."""
    
    def test_get_config_existing_key(self):
        """Test getting existing configuration value."""
        config = {'key1': 'value1', 'key2': 'value2'}
        workflow = ConcreteWorkflow(config)
        
        assert workflow.get_config('key1') == 'value1'
    
    def test_get_config_missing_key_with_default(self):
        """Test getting missing key returns default value."""
        config = {'key1': 'value1', 'key2': 'value2'}
        workflow = ConcreteWorkflow(config)
        
        assert workflow.get_config('nonexistent', 'default') == 'default'
    
    def test_get_config_missing_key_no_default(self):
        """Test getting missing key without default returns None."""
        config = {'key1': 'value1', 'key2': 'value2'}
        workflow = ConcreteWorkflow(config)
        
        assert workflow.get_config('nonexistent') is None
    
    def test_set_config_new_key(self):
        """Test setting new configuration key."""
        config = {'key1': 'value1', 'key2': 'value2'}
        workflow = ConcreteWorkflow(config)
        
        workflow.set_config('key3', 'value3')
        
        assert workflow.get_config('key3') == 'value3'
        assert workflow.config['key3'] == 'value3'
    
    def test_set_config_existing_key(self):
        """Test overwriting existing configuration key."""
        config = {'key1': 'value1', 'key2': 'value2'}
        workflow = ConcreteWorkflow(config)
        
        workflow.set_config('key1', 'new_value')
        
        assert workflow.get_config('key1') == 'new_value'
    
    def test_set_config_various_types(self):
        """Test setting configuration values of various types."""
        config = {'key1': 'value1', 'key2': 'value2'}
        workflow = ConcreteWorkflow(config)
        
        workflow.set_config('int_val', 42)
        workflow.set_config('list_val', [1, 2, 3])
        workflow.set_config('dict_val', {'nested': 'value'})
        workflow.set_config('bool_val', True)
        
        assert workflow.get_config('int_val') == 42
        assert workflow.get_config('list_val') == [1, 2, 3]
        assert workflow.get_config('dict_val') == {'nested': 'value'}
        assert workflow.get_config('bool_val') is True


class TestWorkflowRun:
    """Test Workflow run method."""
    
    def test_run_executes(self):
        """Test that run method executes."""
        config = {'key1': 'value1', 'key2': 'value2'}
        workflow = ConcreteWorkflow(config)
        
        result = workflow.run()
        
        assert result == "workflow_executed"


class TestWorkflowAbstractMethods:
    """Test that abstract methods must be implemented."""
    
    def test_missing_get_required_config_keys(self):
        """Test that subclass must implement get_required_config_keys."""
        class IncompleteWorkflow(Workflow):
            def run(self) -> Any:
                return None
        
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteWorkflow({})
    
    def test_missing_run_method(self):
        """Test that subclass must implement run."""
        class IncompleteWorkflow(Workflow):
            def get_required_config_keys(self) -> List[str]:
                return []
        
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            IncompleteWorkflow({})


class TestWorkflowPrintConfig:
    """Test Workflow print_config method."""
    
    def test_print_config_output(self, capsys):
        """Test that print_config displays configuration."""
        config = {'key1': 'value1', 'key2': 'value2'}
        workflow = ConcreteWorkflow(config)
        
        workflow.print_config()
        
        captured = capsys.readouterr()
        assert 'Configuration:' in captured.out
        assert 'key1: value1' in captured.out
        assert 'key2: value2' in captured.out
    
    def test_print_config_empty(self, capsys):
        """Test print_config with empty configuration."""
        config = {}
        workflow = ConcreteWorkflow(config, required_keys=[])
        
        workflow.print_config()
        
        captured = capsys.readouterr()
        assert 'Configuration:' in captured.out


class TestWorkflowValidation:
    """Test Workflow configuration validation."""
    
    def test_validate_config_called_on_init(self):
        """Test that _validate_config is called during initialization."""
        config = {'key1': 'value1'}
        
        with pytest.raises(KeyError, match="Required keys missing from config"):
            ConcreteWorkflow(config)
    
    def test_validate_config_with_all_required_keys(self):
        """Test validation passes with all required keys present."""
        config = {'key1': 'value1', 'key2': 'value2', 'extra': 'allowed'}
        workflow = ConcreteWorkflow(config)
        
        assert workflow.config == config
    
    def test_validate_config_multiple_missing_keys(self):
        """Test validation error message with multiple missing keys."""
        config = {'key3': 'value3'}
        
        with pytest.raises(KeyError) as excinfo:
            ConcreteWorkflow(config)
        
        assert 'key1' in str(excinfo.value)
        assert 'key2' in str(excinfo.value)

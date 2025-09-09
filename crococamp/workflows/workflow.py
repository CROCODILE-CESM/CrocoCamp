"""Base Workflow class for CrocoCamp workflow orchestration."""

import os
from abc import ABC, abstractmethod

from ..utils import config as config_utils


class Workflow(ABC):
    """Base class for all CrocoCamp workflows.
    
    Provides common functionality for configuration loading, validation,
    and workflow execution interface.
    """
    
    def __init__(self, config):
        """Initialize workflow with configuration.
        
        Args:
            config: Configuration dictionary containing workflow parameters
        """
        self.config = config
        self._validate_config()
    
    @classmethod
    def from_config_file(cls, config_file, **kwargs):
        """Create workflow instance from configuration file.
        
        Args:
            config_file: Path to YAML configuration file
            **kwargs: Additional keyword arguments to override config values
            
        Returns:
            Workflow instance
        """
        config = config_utils.read_config(config_file)
        
        # Override config with any provided kwargs
        config.update(kwargs)
        
        return cls(config)
    
    def _validate_config(self):
        """Validate configuration parameters.
        
        Subclasses should override this method to provide specific validation.
        """
        required_keys = self.get_required_config_keys()
        if required_keys:
            config_utils.validate_config_keys(self.config, required_keys)
    
    @abstractmethod
    def get_required_config_keys(self):
        """Return list of required configuration keys.
        
        Returns:
            List of required configuration key names
        """
        pass
    
    @abstractmethod
    def run(self):
        """Execute the workflow.
        
        Returns:
            Workflow execution result
        """
        pass
    
    def get_config(self, key, default=None):
        """Get configuration value by key.
        
        Args:
            key: Configuration key name
            default: Default value if key not found
            
        Returns:
            Configuration value
        """
        return self.config.get(key, default)
    
    def set_config(self, key, value):
        """Set configuration value.
        
        Args:
            key: Configuration key name
            value: Value to set
        """
        self.config[key] = value
    
    def print_config(self):
        """Print current configuration."""
        print("Configuration:")
        for key, value in self.config.items():
            print(f"  {key}: {value}")
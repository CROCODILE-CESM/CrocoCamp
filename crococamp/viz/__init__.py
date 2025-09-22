"""Visualization tools for CrocoCamp data analysis.

This module provides interactive visualization widgets for analyzing
model-observation comparisons with support for both dask and pandas DataFrames.
"""

from .config import MapConfig, ProfileConfig
from .interactive_map import InteractiveMapWidget
from .interactive_profile import InteractiveProfileWidget

__all__ = ['InteractiveMapWidget', 'InteractiveProfileWidget', 'MapConfig', 'ProfileConfig']
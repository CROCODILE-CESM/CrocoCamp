"""Visualization tools for CrocoCamp data analysis.

This module provides interactive visualization widgets for analyzing
model-observation comparisons with support for both dask and pandas DataFrames.
"""

from .config import MapConfig
from .interactive_map import InteractiveMapWidget

__all__ = ['InteractiveMapWidget', 'MapConfig']
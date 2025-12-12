"""Tests for InteractiveWidgetMap initialization and state management."""

import pytest
import pandas as pd
import dask.dataframe as dd
from unittest.mock import Mock, patch

from crococamp.viz.interactive_widget_map import InteractiveWidgetMap
from crococamp.viz.viz_config import MapConfig


class TestMapWidgetInitialization:
    """Test InteractiveWidgetMap initialization."""
    
    def test_init_with_pandas_dataframe(self):
        """Test initialization with pandas DataFrame."""
        df = pd.DataFrame({
            'longitude': [-170.0, -160.0, -150.0],
            'latitude': [40.0, 45.0, 50.0],
            'vertical': [-10, -20, -30],
            'obs': [20.0, 21.0, 22.0]
        })
        
        with patch.object(InteractiveWidgetMap, '_setup_widget_workflow'):
            widget = InteractiveWidgetMap(df)
            
            assert isinstance(widget.df, pd.DataFrame)
            assert widget.config is not None
            assert isinstance(widget.config, MapConfig)
    
    def test_init_with_dask_dataframe(self):
        """Test initialization with Dask DataFrame."""
        df = pd.DataFrame({
            'longitude': [-170.0, -160.0],
            'latitude': [40.0, 45.0],
            'obs': [20.0, 21.0]
        })
        ddf = dd.from_pandas(df, npartitions=1)
        
        with patch.object(InteractiveWidgetMap, '_setup_widget_workflow'):
            widget = InteractiveWidgetMap(ddf)
            
            assert isinstance(widget.df, dd.DataFrame)
    
    def test_init_with_custom_config(self):
        """Test initialization with custom MapConfig."""
        df = pd.DataFrame({
            'longitude': [-170.0],
            'latitude': [40.0]
        })
        config = MapConfig(colormap='viridis', scatter_size=200)
        
        with patch.object(InteractiveWidgetMap, '_setup_widget_workflow'):
            widget = InteractiveWidgetMap(df, config=config)
            
            assert widget.config is config
            assert widget.config.colormap == 'viridis'
            assert widget.config.scatter_size == 200


class TestMapWidgetStateInitialization:
    """Test _initialize_state method."""
    
    def test_initialize_state_sets_defaults(self):
        """Test that _initialize_state sets default values."""
        df = pd.DataFrame({
            'longitude': [-170.0, -160.0],
            'latitude': [40.0, 45.0],
            'vertical': [-10, -20]
        })
        
        with patch.object(InteractiveWidgetMap, '_setup_widget_workflow'):
            widget = InteractiveWidgetMap(df)
            widget._initialize_state()
            
            assert widget.filtered_df is None
            assert widget.min_time is None
            assert widget.max_time is None
            assert widget.total_hours is None
            assert widget.plot_var is None
            assert widget.plot_title is None


class TestMapWidgetExtentCalculation:
    """Test map extent calculation."""
    
    def test_calculate_extent_with_config_extent(self):
        """Test that config extent is used when provided."""
        df = pd.DataFrame({
            'longitude': [-170.0, -160.0],
            'latitude': [40.0, 45.0]
        })
        config_extent = (-180, -140, 30, 60)
        config = MapConfig(map_extent=config_extent)
        
        with patch.object(InteractiveWidgetMap, '_setup_widget_workflow'):
            widget = InteractiveWidgetMap(df, config=config)
            widget._calculate_map_extent()
            
            assert widget.map_extent == config_extent
    
    def test_calculate_extent_auto_with_padding(self):
        """Test auto-calculation of extent with padding."""
        df = pd.DataFrame({
            'longitude': [-170.0, -160.0],
            'latitude': [40.0, 45.0]
        })
        config = MapConfig(padding=5.0)
        
        with patch.object(InteractiveWidgetMap, '_setup_widget_workflow'):
            widget = InteractiveWidgetMap(df, config=config)
            widget._calculate_map_extent()
            
            # Check that extent was calculated
            assert widget.map_extent is not None
            assert len(widget.map_extent) == 4
            
            # Check that padding was applied
            lon_min, lon_max, lat_min, lat_max = widget.map_extent
            assert lon_min < -170.0
            assert lon_max > -160.0
            assert lat_min < 40.0
            assert lat_max > 45.0
    
    def test_calculate_extent_clamps_to_limits(self):
        """Test that extent is clamped to valid ranges."""
        df = pd.DataFrame({
            'longitude': [-179.0, 179.0],
            'latitude': [-89.0, 89.0]
        })
        config = MapConfig(padding=20.0)
        
        with patch.object(InteractiveWidgetMap, '_setup_widget_workflow'):
            widget = InteractiveWidgetMap(df, config=config)
            widget._calculate_map_extent()
            
            lon_min, lon_max, lat_min, lat_max = widget.map_extent
            
            # Should be clamped to -180/180 and -90/90
            assert lon_min >= -180
            assert lon_max <= 180
            assert lat_min >= -90
            assert lat_max <= 90


class TestMapWidgetVerticalLimits:
    """Test vertical coordinate limits calculation."""
    
    def test_calculate_vertical_limits_with_config(self):
        """Test vertical limits from config."""
        df = pd.DataFrame({
            'vertical': [-10, -20, -30]
        })
        config = MapConfig(vertical_range=(0, 100))
        
        with patch.object(InteractiveWidgetMap, '_setup_widget_workflow'):
            widget = InteractiveWidgetMap(df, config=config)
            
            assert widget.config.vrange is not None
            assert widget.config.vrange['min'] == 0
            assert widget.config.vrange['max'] == 100


class TestMapWidgetConfiguration:
    """Test configuration handling in MapWidget."""
    
    def test_default_config_applied(self):
        """Test that default config is created when none provided."""
        df = pd.DataFrame({
            'longitude': [-170.0],
            'latitude': [40.0]
        })
        
        with patch.object(InteractiveWidgetMap, '_setup_widget_workflow'):
            widget = InteractiveWidgetMap(df)
            
            assert isinstance(widget.config, MapConfig)
            assert widget.config.colormap == 'cividis'
            assert widget.config.default_window_hours == 24
    
    def test_custom_scatter_parameters(self):
        """Test widget with custom scatter parameters."""
        df = pd.DataFrame({
            'longitude': [-170.0],
            'latitude': [40.0]
        })
        config = MapConfig(scatter_size=150, scatter_alpha=0.5)
        
        with patch.object(InteractiveWidgetMap, '_setup_widget_workflow'):
            widget = InteractiveWidgetMap(df, config=config)
            
            assert widget.config.scatter_size == 150
            assert widget.config.scatter_alpha == 0.5
    
    def test_custom_padding(self):
        """Test widget with custom padding."""
        df = pd.DataFrame({
            'longitude': [-170.0],
            'latitude': [40.0]
        })
        config = MapConfig(padding=10.0)
        
        with patch.object(InteractiveWidgetMap, '_setup_widget_workflow'):
            widget = InteractiveWidgetMap(df, config=config)
            
            assert widget.config.padding == 10.0
    
    def test_disallowed_plotvars(self):
        """Test disallowed plot variables configuration."""
        df = pd.DataFrame({
            'longitude': [-170.0],
            'latitude': [40.0],
            'obs': [20.0]
        })
        config = MapConfig()
        
        with patch.object(InteractiveWidgetMap, '_setup_widget_workflow'):
            widget = InteractiveWidgetMap(df, config=config)
            
            assert 'time' in widget.config.disallowed_plotvars
            assert 'longitude' in widget.config.disallowed_plotvars
            assert 'latitude' in widget.config.disallowed_plotvars


class TestMapWidgetDataFrameHandling:
    """Test DataFrame handling in MapWidget."""
    
    def test_empty_dataframe(self):
        """Test initialization with empty DataFrame."""
        df = pd.DataFrame()
        
        with patch.object(InteractiveWidgetMap, '_setup_widget_workflow'):
            widget = InteractiveWidgetMap(df)
            
            assert len(widget.df) == 0
    
    def test_single_row_dataframe(self):
        """Test initialization with single row DataFrame."""
        df = pd.DataFrame({
            'longitude': [-170.0],
            'latitude': [40.0],
            'obs': [20.0]
        })
        
        with patch.object(InteractiveWidgetMap, '_setup_widget_workflow'):
            widget = InteractiveWidgetMap(df)
            
            assert len(widget.df) == 1
    
    def test_large_dataframe(self):
        """Test initialization with large DataFrame."""
        df = pd.DataFrame({
            'longitude': [-170.0] * 10000,
            'latitude': [40.0] * 10000,
            'obs': range(10000)
        })
        
        with patch.object(InteractiveWidgetMap, '_setup_widget_workflow'):
            widget = InteractiveWidgetMap(df)
            
            assert len(widget.df) == 10000
    
    def test_dataframe_with_missing_values(self):
        """Test initialization with DataFrame containing NaN."""
        df = pd.DataFrame({
            'longitude': [-170.0, None, -150.0],
            'latitude': [40.0, 45.0, None],
            'obs': [20.0, None, 22.0]
        })
        
        with patch.object(InteractiveWidgetMap, '_setup_widget_workflow'):
            widget = InteractiveWidgetMap(df)
            
            assert widget.df['longitude'].isna().sum() == 1
            assert widget.df['latitude'].isna().sum() == 1
            assert widget.df['obs'].isna().sum() == 1


class TestMapWidgetLongitudeHandling:
    """Test longitude coordinate handling."""
    
    def test_longitude_range_0_360(self):
        """Test handling of 0-360 longitude range."""
        df = pd.DataFrame({
            'longitude': [0.0, 180.0, 359.0],
            'latitude': [0.0, 0.0, 0.0]
        })
        config = MapConfig(padding=5.0)
        
        with patch.object(InteractiveWidgetMap, '_setup_widget_workflow'):
            widget = InteractiveWidgetMap(df, config=config)
            widget._calculate_map_extent()
            
            # Extent should be calculated
            assert widget.map_extent is not None
    
    def test_longitude_range_minus180_180(self):
        """Test handling of -180 to 180 longitude range."""
        df = pd.DataFrame({
            'longitude': [-180.0, 0.0, 180.0],
            'latitude': [0.0, 0.0, 0.0]
        })
        config = MapConfig(padding=5.0)
        
        with patch.object(InteractiveWidgetMap, '_setup_widget_workflow'):
            widget = InteractiveWidgetMap(df, config=config)
            widget._calculate_map_extent()
            
            assert widget.map_extent is not None


class TestMapWidgetEdgeCases:
    """Test edge cases for MapWidget."""
    
    def test_zero_padding(self):
        """Test widget with zero padding."""
        df = pd.DataFrame({
            'longitude': [-170.0, -160.0],
            'latitude': [40.0, 45.0]
        })
        config = MapConfig(padding=0.0)
        
        with patch.object(InteractiveWidgetMap, '_setup_widget_workflow'):
            widget = InteractiveWidgetMap(df, config=config)
            widget._calculate_map_extent()
            
            # Should still calculate extent, just without padding
            assert widget.map_extent is not None
    
    def test_custom_figure_size(self):
        """Test widget with custom figure size."""
        df = pd.DataFrame({
            'longitude': [-170.0],
            'latitude': [40.0]
        })
        config = MapConfig(figure_size=(15, 10))
        
        with patch.object(InteractiveWidgetMap, '_setup_widget_workflow'):
            widget = InteractiveWidgetMap(df, config=config)
            
            assert widget.config.figure_size == (15, 10)
    
    def test_empty_disallowed_plotvars(self):
        """Test widget with empty disallowed plotvars list."""
        df = pd.DataFrame({
            'longitude': [-170.0],
            'latitude': [40.0]
        })
        config = MapConfig(disallowed_plotvars=[])
        
        with patch.object(InteractiveWidgetMap, '_setup_widget_workflow'):
            widget = InteractiveWidgetMap(df, config=config)
            
            assert widget.config.disallowed_plotvars == []

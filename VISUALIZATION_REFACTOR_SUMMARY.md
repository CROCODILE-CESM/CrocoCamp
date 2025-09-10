# Visualization Module Extraction - Summary

## What was changed

The notebook `examples/model-obs-comparison.ipynb` contained ~370 lines of widget setup, callback functions, and plotting logic that made it difficult to read and reuse. This code has been extracted into a new `crococamp/viz/` module.

## New Module Structure

```
crococamp/viz/
├── __init__.py           # Public API exports
├── config.py             # MapConfig class for customization
└── interactive_map.py    # InteractiveMapWidget main class
```

## Key Features

### InteractiveMapWidget
- Accepts both dask and pandas DataFrames
- Handles all widget creation, callbacks, and plotting internally
- Clean, simple API: `widget = InteractiveMapWidget(dataframe); widget.setup()`

### MapConfig
- Customizable defaults for:
  - colormap (e.g., 'viridis', 'plasma', 'cividis')
  - plot_title 
  - map_extent (custom geographic bounds)
  - figure_size, scatter_size, scatter_alpha
  - default_window_hours
  - disallowed_plotvars

## Usage Examples

### Basic usage (default configuration):
```python
from crococamp.viz import InteractiveMapWidget
widget = InteractiveMapWidget(dataframe)
widget.setup()
```

### With custom configuration:
```python
from crococamp.viz import InteractiveMapWidget, MapConfig

config = MapConfig(
    colormap='viridis',
    plot_title='Custom Analysis',
    figure_size=(20, 12),
    map_extent=(-180, 180, -90, 90)
)
widget = InteractiveMapWidget(dataframe, config)
widget.setup()
```

## Before vs After

**Before**: The notebook contained ~370 lines of complex code with:
- Widget initialization (50+ lines)
- Helper functions (150+ lines) 
- Callback functions (100+ lines)
- Setup and display logic (50+ lines)

**After**: The notebook is now ~30 lines of clean, readable code:
- Load data (5 lines)
- Create and setup widget (10 lines)
- Optional configuration examples (15 lines)

## Benefits

1. **Improved User Experience**: Users see only the essential code, not implementation details
2. **Reusability**: The visualization logic can now be used in other notebooks or scripts
3. **Maintainability**: All widget logic is centralized in one module
4. **Customization**: Easy to modify defaults through configuration
5. **Flexibility**: Works with both dask and pandas DataFrames seamlessly

## Files Created/Modified

- **Created**: `crococamp/viz/__init__.py`
- **Created**: `crococamp/viz/config.py` 
- **Created**: `crococamp/viz/interactive_map.py`
- **Created**: `examples/model-obs-comparison-simplified.ipynb` (demo)
- **Modified**: `examples/model-obs-comparison.ipynb` (simplified)
- **Backup**: `examples/model-obs-comparison-original.ipynb` (original code preserved)
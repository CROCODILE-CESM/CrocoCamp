# obs_seq.in File Splitting Utility

This document describes the obs_seq.in file splitting utility implemented in `crococamp.utils.obs_seq_splitting`.

## Overview

The utility splits DART obs_seq.in files into time windows suitable for use with `perfect_model_obs`, as specified in a YAML configuration. This is useful for processing large observation datasets in smaller time chunks.

## Key Features

1. **Flexible Period Parsing**: Supports multiple formats for specifying averaging periods
2. **Model Time Bounds Detection**: Automatically scans MOM6 NetCDF files to determine time bounds
3. **Observation File Mapping**: Maps obs_seq.in files to their time ranges using pyDARTdiags
4. **Interval Overlap Logic**: Uses precise overlap detection to select files for each window
5. **DART Integration**: Updates namelist files and calls obs_sequence_tool

## Design Decisions

### Interval Overlap Logic

The utility uses the condition:
```python
if not (window_end <= t0_obs or window_start >= t1_obs):
    # intervals overlap, include this file
```

This is the correct and only condition needed to determine if two time intervals overlap. The logic is:
- Two intervals [a,b] and [c,d] do NOT overlap if b ≤ c or a ≥ d
- Therefore they DO overlap if NOT (b ≤ c or a ≥ d)

### Modular Architecture  

The utility is broken into focused functions that can be used independently:
- `parse_averaging_period()`: Parse time period specifications
- `get_model_time_bounds()`: Extract time bounds from model files
- `build_obs_seq_time_map()`: Map observation files to time ranges
- `compute_time_windows()`: Calculate time windows from bounds and period
- `select_files_for_windows()`: Select files for each window using overlap logic
- `split_obs_seq_files()`: Main orchestration function

### Error Handling

The utility uses defensive programming:
- Continues processing if individual files fail to read
- Provides clear error messages for configuration issues
- Validates all required parameters before processing

## Usage

### Basic Usage

```python
from crococamp.utils.obs_seq_splitting import split_obs_seq_files

config = {
    "dart_tools_dir": "/path/to/DART/tools",
    "obs_seq_in_folder": "/path/to/obs_seq_files",
    "model_files_folder": "/path/to/model_files",
    "input_nml_template": "/path/to/input.nml.template",
    "output_folder": "/path/to/output",
    "averaging_period": "monthly"  # or use time_window dict
}

# Process all files
num_processed = split_obs_seq_files(config)
print(f"Processed {num_processed} time windows")
```

### Configuration Options

#### Time Period Specification

**Option 1: String format**
```yaml
averaging_period: "monthly"    # 30 days
averaging_period: "yearly"     # 365 days  
averaging_period: "7D"         # 7 days
averaging_period: "2W"         # 14 days
averaging_period: "12H"        # 12 hours
```

**Option 2: Dictionary format**
```yaml
time_window:
  days: 30
  hours: 12
  minutes: 0
  seconds: 0
  weeks: 0      # converted to days
  months: 0     # converted to days (~30 days/month)
  years: 0      # converted to days (~365 days/year)
```

#### Required Configuration Keys

- `dart_tools_dir`: Path to DART obs_sequence_tool executable directory
- `obs_seq_in_folder`: Directory containing obs_seq.in files  
- `input_nml_template`: Template input.nml file for obs_sequence_tool
- `output_folder`: Output directory for windowed obs_seq files
- `time_window` OR `averaging_period`: Time window specification
- `model_files_folder`: Directory with MOM6 files (if model_time_bounds not provided)

### Advanced Usage

```python
from crococamp.utils.obs_seq_splitting import (
    get_model_time_bounds,
    build_obs_seq_time_map,
    compute_time_windows,
    select_files_for_windows
)

# Pre-compute model time bounds
t0, t1 = get_model_time_bounds("/path/to/model/files")

# Build observation file map
obs_map = build_obs_seq_time_map("/path/to/obs_seq/files")

# Compute windows
windows = compute_time_windows(t0, t1, pd.Timedelta(days=30))

# Select files for each window
window_files = select_files_for_windows(obs_map, windows)

# Process with pre-computed bounds
split_obs_seq_files(config, model_time_bounds=(t0, t1))
```

## Integration with CrocoCamp

The utility leverages existing CrocoCamp infrastructure:

- **File utilities**: Uses `crococamp.io.file_utils` for file operations and time conversion
- **Namelist handling**: Uses `crococamp.utils.namelist.Namelist` for updating input.nml files  
- **Configuration**: Compatible with existing YAML configuration patterns
- **Code style**: Follows CrocoCamp conventions with type hints and docstrings

## Example Configuration File

See `configs/config_time_window.yaml` for a complete example configuration file.

## Error Cases

The utility handles common error scenarios:

1. **No model files found**: Raises ValueError with clear message
2. **No obs_seq.in files found**: Raises ValueError with clear message  
3. **Time span too short**: Raises ValueError if period exceeds total time span
4. **obs_sequence_tool not found**: Raises FileNotFoundError with path
5. **Individual file read errors**: Logs warnings and continues processing
6. **obs_sequence_tool execution errors**: Logs error and continues to next window

## Performance Considerations

- The utility processes files sequentially to avoid overwhelming the filesystem
- Memory usage is minimized by reading observation files one at a time
- Time bounds are cached to avoid re-reading model files
- Error handling allows partial processing rather than failing completely
"""Utilities for splitting obs_seq.in files into time windows for DART perfect_model_obs."""

import os
import re
import subprocess
from datetime import timedelta
from typing import Any, Dict, List, Tuple, Optional, Union
from math import floor

import numpy as np
import pandas as pd
import pydartdiags.obs_sequence.obs_sequence as obsq
import xarray as xr

from ..io.file_utils import get_sorted_files, timestamp_to_days_seconds


def parse_averaging_period(period_spec: Union[str, Dict[str, int]]) -> timedelta:
    """Parse averaging period specification into timedelta.

    Args:
        period_spec: Either a string like "monthly", "yearly", "7D", "30D" etc.,
                    or a dictionary with time components

    Returns:
        timedelta: Parsed time period

    Examples:
        >>> parse_averaging_period("monthly")
        timedelta(days=30)
        >>> parse_averaging_period("7D")
        timedelta(days=7)
        >>> parse_averaging_period({"days": 30, "hours": 12})
        timedelta(days=30, hours=12)
    """
    if isinstance(period_spec, dict):
        return _parse_period_dict(period_spec)

    if isinstance(period_spec, str):
        return _parse_period_string(period_spec)

    raise TypeError(f"Averaging period must be string or dict, got {type(period_spec)}")


def _parse_period_dict(period_dict: Dict[str, int]) -> timedelta:
    """Parse period dictionary into timedelta."""
    # Handle dictionary format similar to convert_time_window
    days_in_week = 7
    days_in_month = 30
    days_in_year = 365

    years = period_dict.get("years", 0)
    months = period_dict.get("months", 0)
    weeks = period_dict.get("weeks", 0)
    days = period_dict.get("days", 0)
    hours = period_dict.get("hours", 0)
    minutes = period_dict.get("minutes", 0)
    seconds = period_dict.get("seconds", 0)

    return timedelta(
        days=days + weeks*days_in_week + months*days_in_month + years*days_in_year,
        hours=hours,
        minutes=minutes,
        seconds=seconds
    )


def _parse_period_string(period_spec: str) -> timedelta:
    """Parse period string into timedelta."""
    period_spec = period_spec.strip().lower()

    # Handle special cases
    special_periods = {
        "monthly": timedelta(days=30),
        "yearly": timedelta(days=365),
        "weekly": timedelta(days=7),
        "daily": timedelta(days=1)
    }
    if period_spec in special_periods:
        return special_periods[period_spec]

    # Handle numeric patterns like "7D", "30D", "2W", etc.
    patterns = [
        (r'(\d+)\s*d(?:ays?)?', 'days'),
        (r'(\d+)\s*w(?:eeks?)?', 'weeks'),
        (r'(\d+)\s*m(?:onths?)?', lambda x: timedelta(days=x*30)),
        (r'(\d+)\s*y(?:ears?)?', lambda x: timedelta(days=x*365)),
        (r'(\d+)\s*h(?:ours?)?', 'hours'),
    ]

    kwargs = {}
    for pattern, unit in patterns:
        match = re.search(pattern, period_spec)
        if match:
            value = int(match.group(1))
            if callable(unit):
                return unit(value)
            kwargs[unit] = value

    if kwargs:
        return timedelta(**kwargs)

    # Try pandas to_timedelta as fallback
    try:
        return pd.to_timedelta(period_spec)
    except Exception as exc:
        raise ValueError(
            f"Could not parse averaging period '{period_spec}': {exc}"
        ) from exc


def get_model_time_bounds(model_files_folder: str) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """Scan MOM6 NetCDF files in folder and determine global start and end timestamps.

    Args:
        model_files_folder: Path to folder containing MOM6 NetCDF files

    Returns:
        Tuple of (t0, t1) as pandas Timestamps representing global time bounds

    Raises:
        ValueError: If no .nc files found or no time data available
    """
    nc_files = get_sorted_files(model_files_folder, "*.nc")

    if not nc_files:
        raise ValueError(f"No .nc files found in {model_files_folder}")

    all_times = []

    for nc_file in nc_files:
        try:
            with xr.open_dataset(nc_file, decode_times=True) as dataset:
                # Handle potential calendar issues
                if 'time' in dataset.coords:
                    # Fix calendar as xarray does not read it consistently
                    if 'calendar' not in dataset['time'].attrs:
                        dataset['time'].attrs['calendar'] = 'proleptic_gregorian'

                    times = dataset['time'].values
                    times = np.atleast_1d(times)
                    all_times.extend([pd.Timestamp(t) for t in times])
        except Exception as exc:
            print(f"Warning: Could not read time from {nc_file}: {exc}")
            continue

    if not all_times:
        raise ValueError(f"No time data found in any .nc files in {model_files_folder}")

    return min(all_times), max(all_times)


def build_obs_seq_time_map(obs_seq_in_folder: str) -> Dict[str, Tuple[pd.Timestamp, pd.Timestamp]]:
    """Build mapping of obs_seq.in files to their time bounds.

    Args:
        obs_seq_in_folder: Path to folder containing obs_seq.in files

    Returns:
        Dictionary mapping {filename: (t0_obs, t1_obs)} where times are pandas Timestamps

    Raises:
        ValueError: If no obs_seq.in files found or none can be read
    """
    obs_files = get_sorted_files(obs_seq_in_folder, "*")

    if not obs_files:
        raise ValueError(f"No files found in {obs_seq_in_folder}")

    obs_time_map = {}

    for obs_file in obs_files:
        try:
            obs_seq = obsq.ObsSequence(obs_file)
            if obs_seq.df.empty:
                print(f"Warning: No observations found in {obs_file}")
                continue

            t0_obs = obs_seq.df['time'].min()
            t1_obs = obs_seq.df['time'].max()

            # Ensure we have Timestamps
            if not isinstance(t0_obs, pd.Timestamp):
                t0_obs = pd.Timestamp(t0_obs)
            if not isinstance(t1_obs, pd.Timestamp):
                t1_obs = pd.Timestamp(t1_obs)

            obs_time_map[obs_file] = (t0_obs, t1_obs)

        except Exception as exc:
            print(f"Warning: Could not read {obs_file}: {exc}")
            continue

    if not obs_time_map:
        raise ValueError(f"No valid obs_seq.in files found in {obs_seq_in_folder}")

    return obs_time_map


def compute_time_windows(t0: pd.Timestamp, t1: pd.Timestamp,
                        period: timedelta) -> List[Tuple[pd.Timestamp, pd.Timestamp]]:
    """Compute time windows based on global time bounds and averaging period.

    Args:
        t0: Global start time
        t1: Global end time
        period: Averaging period as timedelta

    Returns:
        List of (window_start, window_end) tuples as pandas Timestamps
    """
    if period <= timedelta(0):
        raise ValueError("Averaging period must be positive")

    # Calculate number of windows
    total_time = t1 - t0
    n_windows = floor(total_time / period)

    if n_windows <= 0:
        raise ValueError(
            f"Time span ({total_time}) is shorter than averaging period ({period})"
        )

    windows = []
    for j in range(n_windows):
        window_start = t0 + j * period
        window_end = t0 + (j + 1) * period
        windows.append((window_start, window_end))

    return windows


def select_files_for_windows(
        obs_time_map: Dict[str, Tuple[pd.Timestamp, pd.Timestamp]],
        windows: List[Tuple[pd.Timestamp, pd.Timestamp]]
) -> Dict[int, List[str]]:
    """Select obs_seq.in files for each time window based on overlap.

    Args:
        obs_time_map: Mapping from filename to (t0_obs, t1_obs)
        windows: List of (window_start, window_end) tuples

    Returns:
        Dictionary mapping window index to list of selected filenames

    Note:
        Uses the overlap condition: not (window_end <= t0_obs or window_start >= t1_obs)
        This means intervals overlap if they share any time points.
    """
    window_files = {}

    for j, (window_start, window_end) in enumerate(windows):
        selected_files = []

        for filename, (t0_obs, t1_obs) in obs_time_map.items():
            # Check for interval overlap using the specified condition
            if not (window_end <= t0_obs or window_start >= t1_obs):
                # Intervals overlap, include this file
                selected_files.append(filename)

        window_files[j] = selected_files

    return window_files


def update_namelist_for_obs_sequence_tool(input_nml_path: str,
                                        selected_files: List[str],
                                        window_start: pd.Timestamp,
                                        window_end: pd.Timestamp,
                                        output_filename: str) -> None:
    """Update input.nml for obs_sequence_tool with window parameters.

    Args:
        input_nml_path: Path to input.nml template file
        selected_files: List of obs_seq.in files for this window
        window_start: Window start time
        window_end: Window end time
        output_filename: Output filename for this window

    Raises:
        ImportError: If namelist utilities not available
        ValueError: If input.nml file not found
    """
    # Import here to avoid circular imports
    from ..utils.namelist import Namelist  # pylint: disable=import-outside-toplevel

    if not os.path.exists(input_nml_path):
        raise ValueError(f"Input namelist file not found: {input_nml_path}")

    namelist = Namelist(input_nml_path)

    # Update filename_seq with list of selected files
    namelist.update_namelist_param(
        "obs_sequence_tool_nml", "filename_seq", selected_files, string=False
    )

    # Convert times to DART format
    first_days, first_seconds = timestamp_to_days_seconds(np.datetime64(window_start))
    last_days, last_seconds = timestamp_to_days_seconds(np.datetime64(window_end))

    # Update time bounds
    namelist.update_namelist_param(
        "obs_sequence_tool_nml", "first_obs_days", first_days, string=False
    )
    namelist.update_namelist_param(
        "obs_sequence_tool_nml", "first_obs_seconds", first_seconds, string=False
    )
    namelist.update_namelist_param(
        "obs_sequence_tool_nml", "last_obs_days", last_days, string=False
    )
    namelist.update_namelist_param(
        "obs_sequence_tool_nml", "last_obs_seconds", last_seconds, string=False
    )

    # Update output filename
    namelist.update_namelist_param(
        "obs_sequence_tool_nml", "filename_out", output_filename
    )

    # Write updated namelist
    namelist.write_namelist()


def call_obs_sequence_tool(dart_tools_dir: str) -> None:
    """Call DART obs_sequence_tool via subprocess.

    Args:
        dart_tools_dir: Directory containing obs_sequence_tool executable

    Raises:
        FileNotFoundError: If obs_sequence_tool not found
        RuntimeError: If obs_sequence_tool execution fails
    """
    obs_sequence_tool = os.path.join(dart_tools_dir, "obs_sequence_tool")

    if not os.path.isfile(obs_sequence_tool):
        raise FileNotFoundError(f"obs_sequence_tool not found at {obs_sequence_tool}")

    try:
        # Similar to the pattern used in workflow_model_obs.py
        with subprocess.Popen(
            [obs_sequence_tool],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        ) as process:

            # Stream the output line-by-line
            for line in process.stdout:
                print(line, end="")

            # Wait for the process to finish
            process.wait()

            # Check the return code for errors
            if process.returncode != 0:
                stderr_output = process.stderr.read()
                raise RuntimeError(
                    f"obs_sequence_tool failed with return code {process.returncode}: "
                    f"{stderr_output}"
                )

    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Could not execute obs_sequence_tool: {exc}") from exc
    except Exception as exc:
        raise RuntimeError(f"Error running obs_sequence_tool: {exc}") from exc


def generate_window_filename(window_start: pd.Timestamp,
                           window_end: pd.Timestamp,
                           base_name: str = "obs_seq_windowed") -> str:
    """Generate unique filename for a time window.

    Args:
        window_start: Start time of window
        window_end: End time of window
        base_name: Base name for the file

    Returns:
        Unique filename incorporating window times
    """
    start_str = window_start.strftime("%Y%m%d_%H%M%S")
    end_str = window_end.strftime("%Y%m%d_%H%M%S")
    return f"{base_name}_{start_str}_to_{end_str}.out"


def split_obs_seq_files(
        config: Dict[str, Any],
        model_time_bounds: Optional[Tuple[pd.Timestamp, pd.Timestamp]] = None
) -> int:
    """Main function to split obs_seq.in files into time windows.

    Args:
        config: Configuration dictionary with required keys:
               - dart_tools_dir: Path to DART tools directory
               - obs_seq_in_folder: Path to obs_seq.in files
               - input_nml_template: Path to input.nml template
               - output_folder: Output directory
               - time_window or averaging_period: Time window specification
               - model_files_folder: Path to model files (if model_time_bounds not provided)
        model_time_bounds: Optional pre-computed model time bounds as (t0, t1)

    Returns:
        Number of time windows processed

    Raises:
        ValueError: If required configuration keys missing or invalid
        FileNotFoundError: If specified directories/files don't exist
    """
    # Validate configuration
    required_keys = [
        "dart_tools_dir", "obs_seq_in_folder", "input_nml_template", "output_folder"
    ]

    for key in required_keys:
        if key not in config:
            raise ValueError(f"Required configuration key '{key}' missing")

    # Check that we have either time_window or averaging_period
    if "time_window" not in config and "averaging_period" not in config:
        raise ValueError(
            "Either 'time_window' or 'averaging_period' must be specified in config"
        )

    # Parse averaging period
    if "averaging_period" in config:
        period = parse_averaging_period(config["averaging_period"])
    else:
        period = parse_averaging_period(config["time_window"])

    print(f"Using averaging period: {period}")

    # Get model time bounds
    if model_time_bounds is None:
        if "model_files_folder" not in config:
            raise ValueError(
                "Either 'model_files_folder' must be specified in config "
                "or model_time_bounds provided"
            )
        print("Scanning model files for time bounds...")
        t0, t1 = get_model_time_bounds(config["model_files_folder"])
    else:
        t0, t1 = model_time_bounds

    print(f"Model time bounds: {t0} to {t1}")

    # Build obs_seq time map
    print("Building obs_seq.in time map...")
    obs_time_map = build_obs_seq_time_map(config["obs_seq_in_folder"])
    print(f"Found {len(obs_time_map)} obs_seq.in files")

    # Compute time windows
    print("Computing time windows...")
    windows = compute_time_windows(t0, t1, period)
    print(f"Generated {len(windows)} time windows")

    # Select files for each window
    print("Selecting files for each window...")
    window_files = select_files_for_windows(obs_time_map, windows)

    # Create output directory
    os.makedirs(config["output_folder"], exist_ok=True)

    return _process_windows(config, windows, window_files)


def _process_windows(config: Dict[str, Any],
                    windows: List[Tuple[pd.Timestamp, pd.Timestamp]],
                    window_files: Dict[int, List[str]]) -> int:
    """Process each time window."""
    processed_windows = 0
    for j, (window_start, window_end) in enumerate(windows):
        selected_files = window_files[j]

        if not selected_files:
            print(f"Window {j}: No files selected for {window_start} to {window_end}")
            continue

        print(f"Window {j}: Processing {len(selected_files)} files "
              f"for {window_start} to {window_end}")

        # Generate output filename
        output_filename = generate_window_filename(window_start, window_end)
        output_path = os.path.join(config["output_folder"], output_filename)

        # Update namelist
        update_namelist_for_obs_sequence_tool(
            config["input_nml_template"],
            selected_files,
            window_start,
            window_end,
            output_path
        )

        # Call obs_sequence_tool
        try:
            call_obs_sequence_tool(config["dart_tools_dir"])
            print(f"Window {j}: Output saved to {output_path}")
            processed_windows += 1
        except Exception as exc:
            print(f"Window {j}: Error processing - {exc}")
            continue

    print(f"Successfully processed {processed_windows} out of {len(windows)} windows")
    return processed_windows

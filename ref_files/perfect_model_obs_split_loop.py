#!/usr/bin/env python3

## @file main.py
#
#
## @author Enrico Milanese <enrico.milanese@whoi.edu>
#
## @date Thu 03 Jul 2025

##########################################################################
import argparse
from datetime import datetime
import glob
import os
import re
from pathlib import Path
import shutil
import subprocess
import sys
import yaml

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pydartdiags.obs_sequence.obs_sequence as obsq
from scipy.spatial import ConvexHull
from shapely.geometry import Point, Polygon
from shapely.vectorized import contains
import xarray as xr
##########################################################################

def read_config(config_file):
    """Read configuration from YAML file."""
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Config file '{config_file}' does not exist")

    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        return config
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML file: {e}")

#------------------------------------------------------------------------------#
def validate_config_keys(config, required_keys):
    """Validate that all required keys are present in config."""
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise KeyError(f"Required keys missing from config: {missing_keys}")

#------------------------------------------------------------------------------#
def check_directory_not_empty(dir_path, name):
    """Check if directory exists and is not empty."""
    if not os.path.isdir(dir_path):
        raise NotADirectoryError(f"{name} '{dir_path}' does not exist or is not a directory")

    if not os.listdir(dir_path):
        raise ValueError(f"{name} '{dir_path}' is empty")

#------------------------------------------------------------------------------#
def check_nc_files_only(dir_path, name):
    """Check if directory contains only .nc files."""
    all_files = [f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))]

    if not all_files:
        raise ValueError(f"{name} '{dir_path}' does not contain any files")

    nc_files = [f for f in all_files if f.endswith('.nc')]

    if len(nc_files) != len(all_files):
        non_nc_files = [f for f in all_files if not f.endswith('.nc')]
        raise ValueError(f"{name} '{dir_path}' contains non-.nc files: {non_nc_files}")

    if not nc_files:
        raise ValueError(f"{name} '{dir_path}' does not contain any .nc files")

#------------------------------------------------------------------------------#
def check_nc_file(file_path, name):
    """Check if file exists and has .nc extension."""
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"{name} '{file_path}' does not exist")

    if not file_path.endswith('.nc'):
        raise ValueError(f"{name} '{file_path}' is not a .nc file")

#------------------------------------------------------------------------------#
def check_or_create_folder(output_folder, name):
    """Check if folder exists, if not, create it."""
    if os.path.exists(output_folder):
        if not os.path.isdir(output_folder):
            raise NotADirectoryError(f"{name} '{output_folder}' exists but is not a directory")
        if os.listdir(output_folder):
            raise ValueError(f"{name} '{output_folder}' exists but is not empty")
    else:
        try:
            os.makedirs(output_folder, exist_ok=True)
        except OSError as e:
            raise OSError(f"Could not create {name} '{output_folder}': {e}")

#------------------------------------------------------------------------------#
def read_namelist(file_path):
    """Read namelist file and return as string."""
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"Namelist file '{file_path}' does not exist")

    try:
        with open(file_path, 'r') as f:
            return f.read()
    except IOError as e:
        raise IOError(f"Could not read namelist file '{file_path}': {e}")

#------------------------------------------------------------------------------#
def write_namelist(file_path, content):
    """Write content to namelist file."""
    try:
        with open(file_path, 'w') as f:
            f.write(content)
    except IOError as e:
        raise IOError(f"Could not write namelist file '{file_path}': {e}")

#------------------------------------------------------------------------------#
def update_namelist_param(content, section, param, value, string=True):
    """Update a parameter in a namelist section."""
    section_pattern = f'&{section}'

    lines = content.split('\n')
    in_section = False
    updated = False

    for j, line in enumerate(lines):
        if line.strip().startswith(section_pattern):
            in_section = True
            continue

        if in_section and line.strip().startswith('&') and not line.strip().startswith(section_pattern):
            in_section = False
            continue

        if in_section:
            param_pattern = rf'^\s*{re.escape(param)}\s*='
            if re.match(param_pattern, line):
                if string:
                    lines[j] = f'   {param.ljust(27)}= "{value}",'
                else:
                    lines[j] = f'   {param.ljust(27)}= {value},'
                updated = True
                break

    if not updated:
        raise ValueError(f"Parameter '{param}' not found in section '&{section}'")

    return '\n'.join(lines)

#------------------------------------------------------------------------------#
def get_sorted_files(directory, pattern="*"):
    """Get sorted list of files in directory matching pattern."""
    file_pattern = os.path.join(directory, pattern)
    files = glob.glob(file_pattern)
    files = [f for f in files if os.path.isfile(f)]
    return sorted(files)

# #------------------------------------------------------------------------------#
# def validate_file_counts(model_in_files, obs_in_files):
#     """Validate that input and obs folders have the same number of files."""
#     if len(model_in_files) != len(obs_in_files):
#         try:
#             for model_in_file in model_in_files:
#                 with xr.open_dataset(model_in_file) as model_ds:
#                     model_time = model_ds.time.values
#                     if len(model_time) == 1:
#                         error_flag = True

#         raise ValueError(
#             f"Number of files in model_in_folder ({len(model_in_files)}) "
#             f"does not match obs_in_folder ({len(obs_in_files)})"
#         )

#------------------------------------------------------------------------------#
def get_model_boundaries(model_file, margin=0.0):
    """Get geographical boundaries from model input file using convex hull."""

    with xr.open_dataset(model_file) as ds:
        # Extract geographical coordinates from the dataset
        xh = ds['lonh'].values
        yh = ds['lath'].values

        # Build grid and stack coordinates for convex hull calculation
        xh_mesh, yh_mesh = np.meshgrid(xh, yh)
        xh_flat = xh_mesh.flatten()
        yh_flat = yh_mesh.flatten()

        # Remove points of rectangular grid where model was not run
        # (e.g. Pacific when modeling Atlantic)
        # Assuming 'thetao' is a variable in the dataset that indicates valid
        # points as it has yh, xh dimensions
        # ref_var = ds['thetao'].values[0, 0, :, :]  # Shape: (len(yh), len(xh))
        # valid_data = ~np.isnan(ref_var)
        # valid_data = valid_data.flatten()
        # xh_flat = xh_flat[valid_data]
        # yh_flat = yh_flat[valid_data]
        ref_var = ds['wet'].values  # Shape: (len(yh), len(xh))
        valid_data = ref_var==1
        valid_data = valid_data.flatten()
        xh_flat = xh_flat[valid_data]
        yh_flat = yh_flat[valid_data]

        # Convert longitude to 0-360 convention and stack points for polygon
        xh_flat_360 = np.where(xh_flat < 0, xh_flat + 360, xh_flat)
        points = np.column_stack((xh_flat_360, yh_flat))
        if len(points) < 3:
            raise ValueError("Not enough valid points to create convex hull")

        # Calculate convex hull
        hull = ConvexHull(points)
        hull_points = points[hull.vertices]

        # Create shapely polygon for point-in-polygon testing
        hull_polygon = Polygon(hull_points)

        # Get bounding box for reference
        lon_min, lat_min = hull_points.min(axis=0)
        lon_max, lat_max = hull_points.max(axis=0)

        print(f"Model grid convex hull bounding box (lon, lat): "
              f"[{lon_min:.2f}, {lon_max:.2f}], [{lat_min:.2f}, {lat_max:.2f}]")
        print(f"Convex hull has {len(hull_points)} vertices")

        return hull_polygon, hull_points

#------------------------------------------------------------------------------#
def trim_obs_seq_in(obs_in_file, hull_polygon, hull_points, trimmed_obs_file):
    """
    trim obs_seq.in file to preserve only observation within specificed geographical area

    Arguments:
    obs_in_file  -- obs_seq.in file to trim (full path)
    hull_polygon -- shapely Polygon object representing the convex hull
    hull_points -- numpy array of hull vertices for display
    trimmed_obs_file -- filename of trimmed observations file
    """

    # Get bounding box for print
    lon_min, lat_min = hull_points.min(axis=0)
    lon_max, lat_max = hull_points.max(axis=0)
    print("   Convex hull boundaries:")
    print(f"   Longitude: {lon_min:.2f}° to {lon_max:.2f}° (0-360 convention)")
    print(f"   Latitude: {lat_min:.2f}° to {lat_max:.2f}°")

    obs_seq_in = obsq.ObsSequence(obs_in_file)
    obs_lon = obs_seq_in.df['longitude'].values
    obs_lat = obs_seq_in.df['latitude'].values

    print(f"\n   Number of observations before filtering to convex hull: {len(obs_seq_in.df)}")

    # Create a mask for observations inside the convex hull
    within_hull_mask = contains(hull_polygon, obs_lon, obs_lat)
    if not np.any(within_hull_mask):
        raise ValueError("No observations found within the convex hull.")

    obs_seq_in_original_df = obs_seq_in.df.copy()
    obs_seq_in.df = obs_seq_in.df[within_hull_mask].reset_index(drop=True)
    print(f"\n   Number of observations after filtering to convex hull: {len(obs_seq_in.df)}")
    print(f"   Percentage of original observations retained: {len(obs_seq_in.df)/len(obs_seq_in_original_df)*100:.1f}%")

    obs_seq_in.write_obs_seq(trimmed_obs_file)
    print(f"   Trimmed file stored to {trimmed_obs_file}.")

#------------------------------------------------------------------------------#
def timestamp_to_days_seconds(timestamp):
    """Convert YYYYMMDD HH:MM:SS timestamp to number of days, number of
    seconds since 1601-01-01

    Arguments:
    timestamp: timestamp in numpy datetime64 format

    Returns:
    days (int): number of days since 1601-01-01
    seconds (int): number of seconds since (1601-01-01 + days)
    """

    timestamp = timestamp.astype('datetime64[s]').astype(datetime)
    reference_date = datetime(1601, 1, 1)
    time_difference = timestamp - reference_date
    days = time_difference.days
    seconds_in_day = time_difference.seconds

    return days, seconds_in_day

#------------------------------------------------------------------------------#
def get_model_time_in_days_seconds(model_in_file):
    """Get model time in days and seconds from model input file."""

    with xr.open_dataset(model_in_file) as model_ds:
        model_time = model_ds.time.values
    model_time = np.atleast_1d(model_time)
    print(len(model_time))
    print(model_time)
    if len(model_time) > 1:
        raise ValueError(f"Model input file {model_in_file} contains multiple time steps, expected single time step.")
    return timestamp_to_days_seconds(model_time[0])

#------------------------------------------------------------------------------#
def get_obs_time_in_days_seconds(obs_in_file):
    """Get obs_seq.in time in days and seconds from obs input file."""

    obs_in_df = obsq.ObsSequence(obs_in_file)
    t1 = obs_in_df.df.time.min()
    t2 = obs_in_df.df.time.max()
    tmid = pd.Timestamp((t1.value + t2.value) // 2)

    return timestamp_to_days_seconds(np.datetime64(tmid))

#------------------------------------------------------------------------------#
def process_files(config, trim_obs=False, no_matching=False, force_obs_time=False):
    """Function that takes a configuration dictionary and processes files.

    Arguments:
    config -- Dictionary containing the following configuration parameters:
        model_in_folder -- Path to the folder containing model input files
        obs_in_folder   -- Path to the folder containing observation files in DART format
        output_folder   -- Path to the folder where output files will be saved
        template_file   -- Path to the template .nc file
        static_file     -- Path to the static .nc file
        ocean_geometry  -- Path to the ocean geometry .nc file
        input_nml_bck   -- (optional) Path to the backup of input.nml files
        trimmed_obs_folder  -- (optional) Path to store trimmed obs_seq.in files

    trim_obs -- Boolean indicating whether to trim obs_seq.in files to model grid boundaries (default: False)

    no_matching -- Boolean indicating whether to skip time-matching and assume
    1:1 correspondence between model and obs files when sorted (default: False)

    force_obs_time -- Boolean indicating whether to assign observations reference time to model files

    """

    # Extract config values
    model_in_folder = config['model_in_folder']
    obs_in_folder = config['obs_in_folder']
    output_folder = config['output_folder']
    template_file = config['template_file']
    static_file = config['static_file']
    ocean_geometry = config['ocean_geometry']
    try:
        input_nml_bck = config['input_nml_bck']
    except:
        print("No input_nml_bck provided, using default 'input.nml.backup'")
        config['input_nml_bck'] = "input.nml.backup"
        input_nml_bck = config['input_nml_bck']

    if trim_obs:
        try:
            trimmed_obs_folder = config['trimmed_obs_folder']
        except:
            print("No trimmed_obs_folder provided, using default 'trimmed_obs_seq'")
            config['trimmed_obs_folder'] = "trimmed_obs_seq"
            trimmed_obs_folder = config['trimmed_obs_folder']

    print("Configuration:")
    print(f"  model_in_folder: {model_in_folder}")
    print(f"  obs_in_folder: {obs_in_folder}")
    print(f"  output_folder: {output_folder}")
    print(f"  template_file: {template_file}")
    print(f"  static_file: {static_file}")
    print(f"  ocean_geometry: {ocean_geometry}")
    print(f"  input_nml_bck: {input_nml_bck}")
    if trim_obs:
        print(f"  trimmed_obs_folder: {trimmed_obs_folder}")

    # Validating config parameters
    print("Validating model_in_folder...")
    check_directory_not_empty(model_in_folder, "model_in_folder")
    check_nc_files_only(model_in_folder, "model_in_folder")

    print("Validating obs_in_folder...")
    check_directory_not_empty(obs_in_folder, "obs_in_folder")

    print("Validating output_folder...")
    check_or_create_folder(output_folder,"output_folder")

    if trim_obs:
        print("Validating trimmed_obs_folder...")
        check_or_create_folder(trimmed_obs_folder, "trimmed_obs_folder")

    print("Validating input_nml_bck...")
    check_or_create_folder(input_nml_bck, "input_nml_bck")

    print("Validating .nc files for model_nml...")
    check_nc_file(template_file, "template_file")
    check_nc_file(static_file, "static_file")
    check_nc_file(ocean_geometry, "ocean_geometry")

    print("Checking input.nml...")
    # Create backup of input.nml
    try:
        shutil.copy2("input.nml", "input.nml.backup")
        print("Created backup: input.nml.backup")
    except IOError as e:
        raise IOError(f"Could not create backup of input.nml: {e}")

    # Read and update namelist
    namelist_content = read_namelist("input.nml")

    print("Updating &model_nml section...")
    namelist_content = update_namelist_param(
        namelist_content, "model_nml", "template_file", template_file
    )
    namelist_content = update_namelist_param(
        namelist_content, "model_nml", "static_file", static_file
    )
    namelist_content = update_namelist_param(
        namelist_content, "model_nml", "ocean_geometry", ocean_geometry
    )

    # Get and validate file lists
    model_in_files = get_sorted_files(model_in_folder, "*.nc")
    obs_in_files = get_sorted_files(obs_in_folder, "*")
    #validate_file_counts(model_in_files, obs_in_files)

    print(f"Found {len(model_in_files)} files to process")

    if trim_obs:
        # Get model boundaries
        print("Getting model boundaries...")
        hull_polygon, hull_points = get_model_boundaries(ocean_geometry)
        # hull_polygon, hull_points = get_model_boundaries(model_in_files[0])

    if no_matching: # trust the obs and model files match 1:1 when sorted
        for counter, (model_in_file, obs_in_file) in enumerate(zip(model_in_files, obs_in_files)):
            process_model_obs_pair(config, model_in_file, obs_in_file, trim_obs, counter, hull_polygon, hull_points, namelist_content, force_obs_time)

    else: # look for each model-obs pair through time-matching
        # Process each mom6 file
        counter = 0
        used_obs_in_files = []
        for model_in_f in model_in_files:

            # For each mom6 file: open it, get number of snapshots and loop over
            # them. For each snapshot, find the obs_seq.in file whose time range
            # includes the snapshot. If found, slice the snapshot out of the mom6
            # file into a temporary tmp_model_in_file and call perfect_model_obs on
            # the pair (tmp_model_in_file, obs_seq.in). Take note of the obs_seq.in
            # files already used, to skip the check in the next snapshots and mom6
            # files to speed up finding each pair. The temporary files are removed
            # after each call to perfect_model_obs.
            print(f"Processing model file {model_in_f}...")
            with xr.open_dataset(model_in_f, decode_times=False) as ds:
                # fix calendar as xarray does not read it consistently with ncviews
                # for unknown reasons
                ds['time'].attrs['calendar'] = 'proleptic_gregorian'
                ds = xr.decode_cf(ds)
                snapshots_nb = ds.dims['time']
                print(f"    model has {snapshots_nb} snapshots.")
                for t_id, time in enumerate(ds['time'].values):
                    print(f"    processing snapshot {t_id+1} of {snapshots_nb}...")
                    for obs_in_file in obs_in_files:
                        # skip obs_seq.in files already used
                        if obs_in_file in used_obs_in_files:
                            continue
                        obs_in_df = obsq.ObsSequence(obs_in_file)
                        t1 = obs_in_df.df.time.min()
                        t2 = obs_in_df.df.time.max()
                        if t1 <= pd.Timestamp(time) <= t2:
                            counter += 1
                            used_obs_in_files.append(obs_in_file)
                            tmp_model_in_file = model_in_f + "_tmp_" + str(t_id)

                            if snapshots_nb > 1:
                                # Slice out the snapshot into a temporary file
                                ncks = [
                                    "ncks", "-d", f"time,{t_id}",
                                    model_in_f, tmp_model_in_file
                                ]
                                print(f"Calling {' '.join(ncks)}")
                                subprocess.run(ncks, check=True)
                            else:
                                # If only one snapshot, just use the file as is
                                tmp_model_in_file = model_in_f

                            # Call perfect_model_obs on the pair (tmp_model_in_file, obs_in_file)
                            process_model_obs_pair(config, tmp_model_in_file, obs_in_file, trim_obs, counter, hull_polygon, hull_points, namelist_content, force_obs_time)

                            # Remove temporary file if it was created
                            if snapshots_nb > 1:
                                os.remove(tmp_model_in_file)

    return len(model_in_files)

#------------------------------------------------------------------------------#
def process_model_obs_pair(config, model_in_file, obs_in_file, trim_obs, counter, hull_polygon, hull_points, namelist_content, force_obs_time):

    model_in_folder = config['model_in_folder']
    obs_in_folder = config['obs_in_folder']
    output_folder = config['output_folder']
    template_file = config['template_file']
    static_file = config['static_file']
    ocean_geometry = config['ocean_geometry']
    input_nml_bck = config['input_nml_bck']
    trimmed_obs_folder = config['trimmed_obs_folder']

    model_in_filename = os.path.basename(model_in_file)
    obs_in_filename = os.path.basename(obs_in_file)

    file_number = f"{counter:04d}"

    obs_in_file_nml = obs_in_file
    if trim_obs:
        print(f"Trimming obs_seq.in file {obs_in_filename} to model grid boundaries...")
        trimmed_obs_file = os.path.join(trimmed_obs_folder, f"trimmed_obs_seq_{file_number}.in")
        trim_obs_seq_in(obs_in_file, hull_polygon, hull_points, trimmed_obs_file)
        obs_in_file_nml = trimmed_obs_file

    perfect_output_filename = f"perfect_output_{file_number}.nc"
    perfect_output_path = os.path.join(output_folder, perfect_output_filename)

    obs_output_filename = f"obs_seq_{file_number}.out"
    obs_output_path = os.path.join(output_folder, obs_output_filename)

    print(f"Processing file #{counter + 1}:")
    print(f"  Model input file: {model_in_filename}")
    print(f"  Obs input file: {obs_in_file_nml}")
    print(f"  Perfect output file: {perfect_output_filename}")
    print(f"  Obs output file: {obs_output_filename}")

    # Update namelist parameters
    namelist_content = update_namelist_param(
        namelist_content, "perfect_model_obs_nml","input_state_files", model_in_file
    )
    namelist_content = update_namelist_param(
        namelist_content, "perfect_model_obs_nml","output_state_files", perfect_output_path
    )
    namelist_content = update_namelist_param(
        namelist_content, "perfect_model_obs_nml","obs_seq_in_file_name", obs_in_file_nml
    )
    namelist_content = update_namelist_param(
        namelist_content, "perfect_model_obs_nml","obs_seq_out_file_name", obs_output_path
    )

    if not force_obs_time:
        # Assign time to model file
        print("Retrieving model time from model input file and updating namelist...")
        model_time_days, model_time_seconds = get_model_time_in_days_seconds(model_in_file)
        namelist_content = update_namelist_param(
            namelist_content, "perfect_model_obs_nml","init_time_days", model_time_days,
            string=False
        )
        namelist_content = update_namelist_param(
            namelist_content, "perfect_model_obs_nml","init_time_seconds", model_time_seconds,
            string=False
        )
    else:
        # Assign time to model file
        print("Retrieving obs time from obs_seq and updating namelist...")
        obs_time_days, obs_time_seconds = get_obs_time_in_days_seconds(obs_in_file)
        namelist_content = update_namelist_param(
            namelist_content, "perfect_model_obs_nml","init_time_days", obs_time_days,
            string=False
        )
        namelist_content = update_namelist_param(
            namelist_content, "perfect_model_obs_nml","init_time_seconds", obs_time_seconds,
            string=False
        )

    # Write updated namelist
    write_namelist("input.nml", namelist_content)
    input_nml_bck_path = os.path.join(input_nml_bck, f"input.nml_{file_number}.backup")
    write_namelist(input_nml_bck_path, namelist_content)
    print("input.nml modified.")
    print()

    # Call perfect_model_obs
    print("Calling perfect_model_obs...")
    current_dir = os.path.dirname(__file__)
    perfect_model_obs = os.path.join(current_dir, "perfect_model_obs")
    process = subprocess.Popen(
        [perfect_model_obs],
        stdout=subprocess.PIPE,  # Capture stdout
        stderr=subprocess.PIPE,  # Capture stderr (optional)
        text=True                # Decode output as text (Python 3.7+)
    )

    # Stream the output line-by-line
    for line in process.stdout:
        print(line, end="")  # Print each line as it is produced

    # Wait for the process to finish
    process.wait()

    # Check the return code for errors
    if process.returncode != 0:
        raise RuntimeError(f"Error: {process.stderr.read()}")

    print(f"Perfect model output saved to: {perfect_output_path}")
    print(f"obs_seq.out output saved to: {obs_output_path}")

    return

#------------------------------------------------------------------------------#
def merge_pair_to_parquet(perf_obs_file, orig_obs_file, parquet_path):

    # Read obs_sequence files
    perf_obs_out = obsq.ObsSequence(perf_obs_file)
    perf_obs_out.update_attributes_from_df()
    trimmed = obsq.ObsSequence(orig_obs_file)
    trimmed.update_attributes_from_df()

    obs_col = [col for col in trimmed.df.columns.to_list() if col.endswith("_observation") or col=="observation"]
    print(trimmed.df.columns.to_list())
    print(obs_col)
    if len(obs_col) > 1:
        raise ValueError("More than one observation columns found.")
    else:
        trimmed.df = trimmed.df.rename(columns={obs_col[0]:"obs"})
    perf_obs_out.df = perf_obs_out.df.rename(columns={"truth":"model"})

    # Generate unique hash for merging (obs_num is not unique across files, but
    # time is)
    def compute_hash(df, cols, hash_col="hash"):
        concat = df[cols].astype(str).agg('-'.join, axis=1)
        df[hash_col] = pd.util.hash_pandas_object(concat, index=False).astype('int64')
        return df

    trimmed.df = compute_hash(trimmed.df, ['obs_num', 'seconds', 'days'])
    perf_obs_out.df = compute_hash(perf_obs_out.df, ['obs_num', 'seconds', 'days'])

    # Merge DataFrames
    merge_key = "hash"
    trimmed.df = trimmed.df.set_index(merge_key, drop=True)
    perf_obs_out.df = perf_obs_out.df.set_index(merge_key, drop=True)
    ref_cols = ['longitude', 'latitude', 'time', 'vertical', 'type', 'obs_err_var']
    merged = pd.merge(
        trimmed.df[ref_cols + ['obs']],
        perf_obs_out.df[ref_cols + ['model']],
        left_index=True,
        right_index=True,
        how='outer',
        suffixes=('_trim', '_perf')
    )

    # Check that reference columns are indeed identical and deduplicate them
    for col in ref_cols:
        c_trim, c_perf = f"{col}_trim", f"{col}_perf"
        if c_trim in merged and c_perf in merged:
            if merged[c_trim].equals(merged[c_perf]) or np.all(np.isclose(merged[c_trim], merged[c_perf], atol=1e-13)):
                merged = merged.drop(columns=[c_trim])
                merged = merged.rename(columns={c_perf: col})
            else:
                raise ValueError(f"{col}: {c_trim} and {c_perf} not identical. The two files probably do not refer to the same observation space.")
        else:
            raise ValueError(f"{col}: one of {c_trim}, {c_perf} not present in merged DataFrame. The two files probably do not refer to the same observation space.")

    # Sort dataframe by time -> position -> depth
    sort_order = ['time', 'longitude', 'latitude', 'vertical']
    merged = merged.sort_values(by=sort_order)

    # Add diagnostic columns
    merged['residual'] = merged['obs'] - merged['model']
    merged['abs_residual'] = np.abs(merged['residual'])
    merged['normalized_residual'] = merged['residual'] / np.sqrt(merged['obs_err_var'])
    merged['squared_residual'] = merged['residual'] ** 2
    merged['log_likelihood'] = -0.5 * (
        merged['residual'] ** 2 / merged['obs_err_var'] +
        np.log(2 * np.pi * merged['obs_err_var'])
    )

    # Reorder columns
    column_order = [
        'time', 'longitude', 'latitude', 'vertical', 'type',
        'model', 'obs', 'obs_err_var',
    ]
    remaining_cols = [col for col in merged.columns if col not in column_order]
    merged = merged[column_order + remaining_cols]

    ddf = dd.from_pandas(merged)
    name_function = lambda x: f"tmp-model-obs-{x}.parquet"
    append = True
    if not os.listdir(parquet_path):
        append=False # create new dataset if it's the first in the folder,
                     # append otherwise
    ddf.to_parquet(
        parquet_path,
        append=append,
        name_function=name_function,
        write_metadata_file=True,
        ignore_divisions=True
    )

    return

#------------------------------------------------------------------------------#
def merge_model_obs_to_parquet(config, trim_obs):
    output_folder = config['output_folder']
    parquet_folder = config['parquet_folder']
    if trim_obs:
        obs_folder = config['trimmed_obs_folder']
    else:
        obs_folder = config['obs_in_folder']

    print("Validating parquet_folder...")
    check_or_create_folder(parquet_folder, "parquet_folder")

    perf_obs_files = sorted(
        glob.glob(os.path.join(output_folder, "obs_seq*.out"))
    )
    orig_obs_files = sorted(
        glob.glob(os.path.join(obs_folder, "*obs_seq*.in"))
    )
    print("perf_obs_files")
    print(perf_obs_files)
    print("orig_obs_files")
    print(orig_obs_files)

    tmp_parquet_folder = os.path.join(parquet_folder, "tmp")
    os.makedirs(tmp_parquet_folder, exist_ok=True)

    for perf_obs_f, orig_obs_f in zip(perf_obs_files, orig_obs_files):
        merge_pair_to_parquet(perf_obs_f, orig_obs_f, tmp_parquet_folder)

    ddf = dd.read_parquet(tmp_parquet_folder)
    ddf = ddf.repartition(partition_size="300MB")
    name_function = lambda x: f"model-obs-{x}.parquet"
    ddf.to_parquet(
        parquet_folder,
        append=False,
        name_function=name_function
    )

    shutil.rmtree(tmp_parquet_folder)

    return

#------------------------------------------------------------------------------#
def main():

    parser = argparse.ArgumentParser(
        description='Script to call perfect_model_obs on multiple model and obs_seq.in files.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python process_config.py config.yaml
  python process_config.py /path/to/myconfig.yaml
        """
    )

    parser.add_argument(
        '-c', '--config',
        type=str,
        help="Path to configuration file (default: config.yaml)",
        required=False,
        default='./config.yaml'
    )

    parser.add_argument(
        '-t', '--trim',
        action='store_true',
        help="Trim obs_seq.in files to model grid boundaries (default: False)",
        required=False,
        default=False
    )

    parser.add_argument(
        '--no-matching',
        action='store_true',
        help="If the obs and model files match 1:1 when alphabetically sorted, skip pair-building through time-matching (faster; default: False)",
        required=False,
        default=False
    )

    parser.add_argument(
        '--force-obs-time',
        action='store_true',
        help="Assign observations reference time to model file in model-obs files pair. Generally discouraged, but relevant when the real model time is not significant (default: False)",
        required=False,
        default=False
    )

    parser.add_argument(
        '--parquet-only',
        action='store_true',
        help="Skip building perfect obs and directly convert existing ones to parquet (default: False)",
        required=False,
        default=False
    )

    args = parser.parse_args()
    if args.parquet_only and args.trim:
        print("Warning: -t/--trim has no effect when --parquet-only is used.")

    config_file = args.config
    trim_obs = args.trim
    no_matching = args.no_matching
    force_obs_time = args.force_obs_time

    try:
        print(f"Reading configuration from: {config_file}")

        # Read and validate config
        config = read_config(config_file)
        required_keys = ['model_in_folder', 'obs_in_folder', 'output_folder',
                         'template_file', 'static_file', 'ocean_geometry']

        validate_config_keys(config, required_keys)

        if not args.parquet_only:
            # Process files
            files_processed = process_files(
                config,
                trim_obs=trim_obs,
                no_matching=no_matching,
                force_obs_time=force_obs_time
            )

            print(f"Total files processed: {files_processed}")
            print("Backup saved as: input.nml.backup")


        # Convert to parquet
        print("Converting obs_seq format to parquet and adding some diagnostics data...")
        merge_model_obs_to_parquet(config, trim_obs)
        print(f"Parquet data save to: {config['parquet_folder']}")

        print("Script executed succesfully.")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

##########################################################################
if __name__ == "__main__":
    main()

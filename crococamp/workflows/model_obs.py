"""Model-observation comparison workflow orchestration for CrocoCamp."""

import os
import shutil
import subprocess

import pandas as pd
import pydartdiags.obs_sequence.obs_sequence as obsq
import xarray as xr

from ..io.file_utils import get_sorted_files, get_model_time_in_days_seconds, get_obs_time_in_days_seconds
from ..io.model_grid import get_model_boundaries
from ..io.obs_seq import trim_obs_seq_in, merge_model_obs_to_parquet
from ..utils.config import (
    check_directory_not_empty, check_nc_files_only, check_or_create_folder,
    check_nc_file
)
from ..utils.namelist import read_namelist, write_namelist, update_namelist_param


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

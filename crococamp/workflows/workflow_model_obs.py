"""Model-observation comparison workflow for CrocoCamp."""

import glob
import os
import shutil
import subprocess

import dask.dataframe as dd
import numpy as np
import pandas as pd
import pydartdiags.obs_sequence.obs_sequence as obsq
import xarray as xr

from . import workflow
from ..io import file_utils
from ..io import model_tools  
from ..io import obs_seq_tools
from ..utils import config as config_utils
from ..utils import namelist


class WorkflowModelObs(workflow.Workflow):
    """Model-observation comparison workflow.
    
    Orchestrates the comparison between ocean model outputs and observation datasets,
    including trimming observations to model grid boundaries, running perfect_model_obs,
    and converting results to parquet format for analysis.
    """
    
    def get_required_config_keys(self):
        """Return list of required configuration keys."""
        return [
            'model_files_folder', 
            'obs_seq_in_folder', 
            'output_folder',
            'template_file', 
            'static_file', 
            'ocean_geometry',
            'perfect_model_obs_dir', 
            'parquet_folder'
        ]
    
    def run(self, trim_obs=False, no_matching=False, 
            force_obs_time=False, parquet_only=False):
        """Execute the complete model-observation workflow.
        
        Args:
            trim_obs: Whether to trim obs_seq.in files to model grid boundaries
            no_matching: Whether to skip time-matching and assume 1:1 correspondence
            force_obs_time: Whether to assign observations reference time to model files
            parquet_only: Whether to skip building perfect obs and directly convert to parquet
            
        Returns:
            Number of files processed
        """
        files_processed = 0
        
        if not parquet_only:
            files_processed = self.process_files(
                trim_obs=trim_obs,
                no_matching=no_matching,
                force_obs_time=force_obs_time
            )
        
        # Convert to parquet
        print("Converting obs_seq format to parquet and adding some diagnostics data...")
        self.merge_model_obs_to_parquet(trim_obs)
        print(f"Parquet data saved to: {self.config['parquet_folder']}")
        
        return files_processed
    
    def process_files(self, trim_obs=False, no_matching=False, 
                     force_obs_time=False):
        """Process model and observation files.
        
        Args:
            trim_obs: Whether to trim obs_seq.in files to model grid boundaries
            no_matching: Whether to skip time-matching and assume 1:1 correspondence
            force_obs_time: Whether to assign observations reference time to model files
            
        Returns:
            Number of files processed
        """

        # Check that perfect_model_obs_dir is set
        if self.config.get('perfect_model_obs_dir') is None:
            raise ValueError("Configuration parameter 'perfect_model_obs_dir' missing: did you specify the path to the perfect_model_obs executable?")

        # Print configuration
        self._print_workflow_config(trim_obs)
        
        # Validate configuration parameters
        self._validate_workflow_paths(trim_obs)
        
        # Setup namelist
        input_nml = os.path.join(self.config['perfect_model_obs_dir'], "input.nml")
        print("Setting up symlink for input.nml...")
        namelist.symlink_to_namelist(input_nml)
        
        try:
            # Create backup and read namelist
            shutil.copy2(input_nml, "input.nml.backup")
            print("Created backup: input.nml.backup")
            namelist_content = namelist.read_namelist(input_nml)
            
            # Update model_nml section
            print("Updating &model_nml section...")
            namelist_content = self._update_model_namelist(namelist_content)
            
            # Get and validate file lists
            model_in_files = file_utils.get_sorted_files(self.config['model_files_folder'], "*.nc")
            obs_in_files = file_utils.get_sorted_files(self.config['obs_seq_in_folder'], "*")
            
            print(f"Found {len(model_in_files)} files to process")
            
            # Get model boundaries if trimming observations
            hull_polygon, hull_points = None, None
            if trim_obs:
                print("Getting model boundaries...")
                hull_polygon, hull_points = model_tools.get_model_boundaries(self.config['ocean_geometry'])
            
            # Process files
            if no_matching:
                counter = 0
                for model_in_file, obs_in_file in zip(model_in_files, obs_in_files):
                    self._process_model_obs_pair(
                        model_in_file, obs_in_file, trim_obs, counter, 
                        hull_polygon, hull_points, namelist_content, force_obs_time
                    )
                    counter += 1
            else:
                counter = self._process_with_time_matching(
                    model_in_files, obs_in_files, trim_obs,
                    hull_polygon, hull_points, namelist_content, force_obs_time
                )
                
        finally:
            # Cleanup
            namelist.cleanup_namelist_symlink()
        
        return len(model_in_files)
    
    def merge_model_obs_to_parquet(self, trim_obs):
        """Merge model and observation files to parquet format."""
        output_folder = self.config['output_folder']
        parquet_folder = self.config['parquet_folder']
        
        if trim_obs:
            obs_folder = self.config['trimmed_obs_folder']
        else:
            obs_folder = self.config['obs_seq_in_folder']

        print("Validating parquet_folder...")
        config_utils.check_or_create_folder(parquet_folder, "parquet_folder")

        perf_obs_files = sorted(glob.glob(os.path.join(output_folder, "obs_seq*.out")))
        orig_obs_files = sorted(glob.glob(os.path.join(obs_folder, "*obs_seq*.in")))
        
        print("perf_obs_files")
        print(perf_obs_files)
        print("orig_obs_files")
        print(orig_obs_files)

        tmp_parquet_folder = os.path.join(parquet_folder, "tmp")
        os.makedirs(tmp_parquet_folder, exist_ok=True)

        for perf_obs_f, orig_obs_f in zip(perf_obs_files, orig_obs_files):
            self._merge_pair_to_parquet(perf_obs_f, orig_obs_f, tmp_parquet_folder)

        ddf = dd.read_parquet(tmp_parquet_folder)
        ddf = ddf.repartition(partition_size="300MB")
        name_function = lambda x: f"model-obs-{x}.parquet"
        ddf.to_parquet(
            parquet_folder,
            append=False,
            name_function=name_function
        )

        shutil.rmtree(tmp_parquet_folder)
    
    def _print_workflow_config(self, trim_obs):
        """Print workflow configuration."""
        print("Configuration:")
        print(f"  perfect_model_obs_dir: {self.config['perfect_model_obs_dir']}")
        input_nml = os.path.join(self.config['perfect_model_obs_dir'], "input.nml")
        print(f"  input_nml: {input_nml}")
        print(f"  model_files_folder: {self.config['model_files_folder']}")
        print(f"  obs_seq_in_folder: {self.config['obs_seq_in_folder']}")
        print(f"  output_folder: {self.config['output_folder']}")
        print(f"  template_file: {self.config['template_file']}")
        print(f"  static_file: {self.config['static_file']}")
        print(f"  ocean_geometry: {self.config['ocean_geometry']}")
        print(f"  input_nml_bck: {self.config.get('input_nml_bck', 'input.nml.backup')}")
        if trim_obs:
            print(f"  trimmed_obs_folder: {self.config.get('trimmed_obs_folder', 'trimmed_obs_seq')}")
    
    def _validate_workflow_paths(self, trim_obs):
        """Validate workflow paths and create necessary directories."""
        # Validate input directories
        print("Validating model_files_folder...")
        config_utils.check_directory_not_empty(self.config['model_files_folder'], "model_files_folder")
        config_utils.check_nc_files_only(self.config['model_files_folder'], "model_files_folder")

        print("Validating obs_seq_in_folder...")
        config_utils.check_directory_not_empty(self.config['obs_seq_in_folder'], "obs_seq_in_folder")

        print("Validating output_folder...")
        config_utils.check_or_create_folder(self.config['output_folder'], "output_folder")

        if trim_obs:
            print("Validating trimmed_obs_folder...")
            trimmed_obs_folder = self.config.get('trimmed_obs_folder', 'trimmed_obs_seq')
            self.config['trimmed_obs_folder'] = trimmed_obs_folder
            config_utils.check_or_create_folder(trimmed_obs_folder, "trimmed_obs_folder")

        # Set default backup folder
        input_nml_bck = self.config.get('input_nml_bck', 'input.nml.backup')
        self.config['input_nml_bck'] = input_nml_bck
        
        print("Validating input_nml_bck...")
        config_utils.check_or_create_folder(input_nml_bck, "input_nml_bck")

        print("Validating .nc files for model_nml...")
        config_utils.check_nc_file(self.config['template_file'], "template_file")
        config_utils.check_nc_file(self.config['static_file'], "static_file")
        config_utils.check_nc_file(self.config['ocean_geometry'], "ocean_geometry")
    
    def _update_model_namelist(self, namelist_content):
        """Update model namelist parameters."""
        namelist_content = namelist.update_namelist_param(
            namelist_content, "model_nml", "template_file", self.config['template_file']
        )
        namelist_content = namelist.update_namelist_param(
            namelist_content, "model_nml", "static_file", self.config['static_file']
        )
        namelist_content = namelist.update_namelist_param(
            namelist_content, "model_nml", "ocean_geometry", self.config['ocean_geometry']
        )
        return namelist_content
    
    def _process_with_time_matching(self, model_in_files, obs_in_files,
                                  trim_obs, hull_polygon, 
                                  hull_points, namelist_content, 
                                  force_obs_time):
        """Process files with time-matching logic."""
        counter = 0
        used_obs_in_files = []
        
        for model_in_f in model_in_files:
            print(f"Processing model file {model_in_f}...")
            with xr.open_dataset(model_in_f, decode_times=False) as ds:
                # Fix calendar as xarray does not read it consistently with ncviews
                ds['time'].attrs['calendar'] = 'proleptic_gregorian'
                ds = xr.decode_cf(ds)
                snapshots_nb = ds.dims['time']
                print(f"    model has {snapshots_nb} snapshots.")
                
                for t_id, time in enumerate(ds['time'].values):
                    print(f"    processing snapshot {t_id+1} of {snapshots_nb}...")
                    for obs_in_file in obs_in_files:
                        # Skip obs_seq.in files already used
                        if obs_in_file in used_obs_in_files:
                            continue
                        
                        obs_in_df = obsq.ObsSequence(obs_in_file)
                        t1 = obs_in_df.df.time.min()
                        t2 = obs_in_df.df.time.max()
                        
                        if t1 <= pd.Timestamp(time) <= t2:
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

                            # Call perfect_model_obs on the pair
                            self._process_model_obs_pair(
                                tmp_model_in_file, obs_in_file, trim_obs, counter,
                                hull_polygon, hull_points, namelist_content, force_obs_time
                            )

                            # Remove temporary file if it was created
                            if snapshots_nb > 1:
                                os.remove(tmp_model_in_file)
                            
                            counter += 1
                            break
        
        return counter
    
    def _process_model_obs_pair(self, model_in_file, obs_in_file, 
                               trim_obs, counter, hull_polygon,
                               hull_points, namelist_content, 
                               force_obs_time):
        """Process a single model-observation file pair."""
        input_nml = os.path.join(self.config['perfect_model_obs_dir'], "input.nml")
        model_in_filename = os.path.basename(model_in_file)
        obs_in_filename = os.path.basename(obs_in_file)
        file_number = f"{counter:04d}"

        obs_in_file_nml = obs_in_file
        if trim_obs:
            print(f"Trimming obs_seq.in file {obs_in_filename} to model grid boundaries...")
            trimmed_obs_file = os.path.join(
                self.config['trimmed_obs_folder'], 
                f"trimmed_obs_seq_{file_number}.in"
            )
            obs_seq_tools.trim_obs_seq_in(obs_in_file, hull_polygon, hull_points, trimmed_obs_file)
            obs_in_file_nml = trimmed_obs_file

        perfect_output_filename = f"perfect_output_{file_number}.nc"
        perfect_output_path = os.path.join(self.config['output_folder'], perfect_output_filename)

        obs_output_filename = f"obs_seq_{file_number}.out"
        obs_output_path = os.path.join(self.config['output_folder'], obs_output_filename)

        print(f"Processing file #{counter + 1}:")
        print(f"  Model input file: {model_in_filename}")
        print(f"  Obs input file: {obs_in_file_nml}")
        print(f"  Perfect output file: {perfect_output_filename}")
        print(f"  Obs output file: {obs_output_filename}")

        # Update namelist parameters
        namelist_content = namelist.update_namelist_param(
            namelist_content, "perfect_model_obs_nml", "input_state_files", model_in_file
        )
        namelist_content = namelist.update_namelist_param(
            namelist_content, "perfect_model_obs_nml", "output_state_files", perfect_output_path
        )
        namelist_content = namelist.update_namelist_param(
            namelist_content, "perfect_model_obs_nml", "obs_seq_in_file_name", obs_in_file_nml
        )
        namelist_content = namelist.update_namelist_param(
            namelist_content, "perfect_model_obs_nml", "obs_seq_out_file_name", obs_output_path
        )

        if not force_obs_time:
            # Assign time to model file
            print("Retrieving model time from model input file and updating namelist...")
            model_time_days, model_time_seconds = file_utils.get_model_time_in_days_seconds(model_in_file)
            namelist_content = namelist.update_namelist_param(
                namelist_content, "perfect_model_obs_nml", "init_time_days", model_time_days,
                string=False
            )
            namelist_content = namelist.update_namelist_param(
                namelist_content, "perfect_model_obs_nml", "init_time_seconds", model_time_seconds,
                string=False
            )
        else:
            # Assign time to model file
            print("Retrieving obs time from obs_seq and updating namelist...")
            obs_time_days, obs_time_seconds = file_utils.get_obs_time_in_days_seconds(obs_in_file)
            namelist_content = namelist.update_namelist_param(
                namelist_content, "perfect_model_obs_nml", "init_time_days", obs_time_days,
                string=False
            )
            namelist_content = namelist.update_namelist_param(
                namelist_content, "perfect_model_obs_nml", "init_time_seconds", obs_time_seconds,
                string=False
            )

        # Write updated namelist
        namelist.write_namelist(input_nml, namelist_content)
        input_nml_bck_path = os.path.join(
            self.config['input_nml_bck'], 
            f"input.nml_{file_number}.backup"
        )
        namelist.write_namelist(input_nml_bck_path, namelist_content)
        print("input.nml modified.")
        print()

        # Call perfect_model_obs
        print("Calling perfect_model_obs...")
        perfect_model_obs = os.path.join(self.config['perfect_model_obs_dir'], "perfect_model_obs")
        process = subprocess.Popen(
            [perfect_model_obs],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        # Stream the output line-by-line
        for line in process.stdout:
            print(line, end="")

        # Wait for the process to finish
        process.wait()

        # Check the return code for errors
        if process.returncode != 0:
            raise RuntimeError(f"Error: {process.stderr.read()}")

        print(f"Perfect model output saved to: {perfect_output_path}")
        print(f"obs_seq.out output saved to: {obs_output_path}")
    
    def _merge_pair_to_parquet(self, perf_obs_file, orig_obs_file, 
                              parquet_path):
        """Merge a pair of observation files into parquet format."""
        # Read obs_sequence files
        perf_obs_out = obsq.ObsSequence(perf_obs_file)
        perf_obs_out.update_attributes_from_df()
        trimmed = obsq.ObsSequence(orig_obs_file)
        trimmed.update_attributes_from_df()

        obs_col = [col for col in trimmed.df.columns.to_list() if col.endswith("_observation") or col=="observation"]
        if len(obs_col) > 1:
            raise ValueError("More than one observation columns found.")
        else:
            trimmed.df = trimmed.df.rename(columns={obs_col[0]:"obs"})

        qc_col = [col for col in perf_obs_out.df.columns.to_list() if col.endswith("_QC")]
        if len(qc_col) > 1:
            raise ValueError("More than one QC column found.")
        perf_model_col = 'perfect_model'
        perf_model_col_QC = perf_model_col + "_QC"
        perf_obs_out.df = perf_obs_out.df.rename(
            columns={
                "truth":perf_model_col,
                qc_col[0]:perf_model_col_QC
            })

        # Generate unique hash for merging
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
            perf_obs_out.df[ref_cols + [perf_model_col, perf_model_col_QC]],
            left_index=True,
            right_index=True,
            how='outer',
            suffixes=('_trim', '_perf')
        )

        # Check that reference columns are identical and deduplicate them
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
        merged['residual'] = merged['obs'] - merged[perf_model_col]
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
            perf_model_col, 'obs', 'obs_err_var',
        ]
        remaining_cols = [col for col in merged.columns if col not in column_order]
        merged = merged[column_order + remaining_cols]

        ddf = dd.from_pandas(merged)
        name_function = lambda x: f"tmp-model-obs-{x}.parquet"
        append = True
        if not os.listdir(parquet_path):
            append = False  # create new dataset if it's the first in the folder

        ddf.to_parquet(
            parquet_path,
            append=append,
            name_function=name_function,
            write_metadata_file=True,
            ignore_divisions=True
        )

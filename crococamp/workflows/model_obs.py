"""Model-observation comparison workflow orchestration for CrocoCamp.

This module provides backward compatibility with the original function-based API
while the new class-based API is recommended for new code.
"""

from .workflow_model_obs import WorkflowModelObs


def process_files(config, trim_obs=False, no_matching=False, force_obs_time=False):
    """Function that takes a configuration dictionary and processes files.
    
    This is a backward compatibility wrapper around WorkflowModelObs.
    For new code, use WorkflowModelObs class directly.

    Arguments:
    config -- Dictionary containing the following configuration parameters:
        model_files_folder -- Path to the folder containing model input files
        obs_seq_in_folder   -- Path to the folder containing observation files in DART format
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
    
    Returns:
        Number of files processed
    """
    workflow = WorkflowModelObs(config)
    return workflow.process_files(trim_obs=trim_obs, no_matching=no_matching, force_obs_time=force_obs_time)


def merge_model_obs_to_parquet(config, trim_obs):
    """Merge model and observation files to parquet format.
    
    This is a backward compatibility wrapper around WorkflowModelObs.
    For new code, use WorkflowModelObs class directly.
    
    Arguments:
    config -- Configuration dictionary
    trim_obs -- Boolean indicating whether trimmed observations were used
    """
    workflow = WorkflowModelObs(config)
    workflow.merge_model_obs_to_parquet(trim_obs)


def merge_pair_to_parquet(perf_obs_file, orig_obs_file, parquet_path):
    """Merge a pair of observation files into parquet format.
    
    This is a backward compatibility wrapper around WorkflowModelObs.
    For new code, use WorkflowModelObs class directly.
    """
    # Create a minimal config just for this operation
    config = {
        'model_files_folder': '',
        'obs_seq_in_folder': '', 
        'output_folder': '',
        'template_file': '',
        'static_file': '', 
        'ocean_geometry': '',
        'perfect_model_obs_dir': '',
        'parquet_folder': ''
    }
    workflow = WorkflowModelObs(config)
    workflow._merge_pair_to_parquet(perf_obs_file, orig_obs_file, parquet_path)

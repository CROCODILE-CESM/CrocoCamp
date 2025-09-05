"""Command-line interface for perfect model observation workflows."""

import argparse
import sys

from ..utils.config import read_config, validate_config_keys
from ..workflows.model_obs import process_files, merge_model_obs_to_parquet


def main():
    """Main CLI entry point for perfect model observation processing."""
    
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
                         'template_file', 'static_file', 'ocean_geometry',
                         'perfect_model_obs_dir', 'parquet_folder']

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


if __name__ == "__main__":
    main()

"""Main CLI entry point for CrocoCamp tools."""

import argparse
import sys
from typing import Optional

from . import perfect_model_obs
from ..io.time_averaging import time_average_from_config


def create_parser() -> argparse.ArgumentParser:
    """Create the main argument parser with subcommands."""
    
    parser = argparse.ArgumentParser(
        description='CrocoCamp - Tools for comparing ocean models, observations, and gridded products',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(
        dest='command',
        help='Available commands',
        metavar='COMMAND'
    )
    
    # Perfect model obs subcommand
    perfect_obs_parser = subparsers.add_parser(
        'perfect-model-obs',
        help='Run perfect model observation workflow',
        description='Script to call perfect_model_obs on multiple model and obs_seq.in files.'
    )
    
    perfect_obs_parser.add_argument(
        '-c', '--config',
        type=str,
        help="Path to configuration file (default: config.yaml)",
        required=False,
        default='./config.yaml'
    )
    
    perfect_obs_parser.add_argument(
        '-t', '--trim',
        action='store_true',
        help="Trim obs_seq.in files to model grid boundaries (default: False)",
        required=False,
        default=False
    )
    
    perfect_obs_parser.add_argument(
        '--no-matching',
        action='store_true',
        help="If the obs and model files match 1:1 when alphabetically sorted, skip pair-building through time-matching (faster; default: False)",
        required=False,
        default=False
    )
    
    perfect_obs_parser.add_argument(
        '--force-obs-time',
        action='store_true',
        help="Assign observations reference time to model file in model-obs files pair. Generally discouraged, but relevant when the real model time is not significant (default: False)",
        required=False,
        default=False
    )
    
    perfect_obs_parser.add_argument(
        '--parquet-only',
        action='store_true',
        help="Skip building perfect obs and directly convert existing ones to parquet (default: False)",
        required=False,
        default=False
    )
    
    perfect_obs_parser.add_argument(
        '--clear-output',
        action='store_true',
        help="Clear all output folders defined in config file before processing files (default: False)",
        required=False,
        default=False
    )
    
    # Time averaging subcommand
    time_avg_parser = subparsers.add_parser(
        'time-average',
        help='Perform time averaging on MOM6 NetCDF files',
        description='Perform configurable time-averaging (resampling and rolling mean) on MOM6 NetCDF output files.'
    )
    
    time_avg_parser.add_argument(
        'config',
        type=str,
        help="Path to time-averaging configuration YAML file"
    )
    
    return parser


def run_perfect_model_obs(args: argparse.Namespace) -> None:
    """Run perfect model observation workflow.
    
    Args:
        args: Parsed command line arguments
    """
    if args.parquet_only and args.trim:
        print("Warning: -t/--trim has no effect when --parquet-only is used.")

    config_file = args.config
    trim_obs = args.trim
    no_matching = args.no_matching
    force_obs_time = args.force_obs_time
    clear_output = args.clear_output

    print(f"Reading configuration from: {config_file}")

    # Import here to avoid dependency issues if not needed
    from ..workflows.workflow_model_obs import WorkflowModelObs
    
    # Create workflow instance
    workflow = WorkflowModelObs.from_config_file(config_file)

    # Validate that perfect_model_obs_dir is specified
    if workflow.get_config('perfect_model_obs_dir') is None:
        raise ValueError("perfect_model_obs_dir must be specified in the config file")

    # Run the workflow
    files_processed = workflow.run(
        trim_obs=trim_obs,
        no_matching=no_matching,
        force_obs_time=force_obs_time,
        parquet_only=args.parquet_only,
        clear_output=clear_output
    )

    if not args.parquet_only:
        print(f"Total files processed: {files_processed}")
        print("Backup saved as: input.nml.backup")

    print("Script executed successfully.")


def run_time_averaging(args: argparse.Namespace) -> None:
    """Run time averaging workflow.
    
    Args:
        args: Parsed command line arguments
    """
    try:
        output_files = time_average_from_config(args.config)
        print(f"Time averaging completed successfully.")
        print(f"Generated {len(output_files)} output files.")
    except Exception as e:
        print(f"Error during time averaging: {e}")
        sys.exit(1)


def main() -> None:
    """Main CLI entry point."""
    parser = create_parser()
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        sys.exit(1)
    
    if args.command == 'perfect-model-obs':
        run_perfect_model_obs(args)
    elif args.command == 'time-average':
        run_time_averaging(args)
    else:
        print(f"Unknown command: {args.command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
"""Main CLI entry point for CrocoCamp tools."""

import argparse
import sys
from typing import Optional

from ..io.mom6_time_averager import time_average_from_config
from ..utils.cli import add_perfect_model_obs_arguments, run_perfect_model_obs_from_args


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
    
    # Use shared argument functions to avoid duplication
    add_perfect_model_obs_arguments(perfect_obs_parser)
    
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


def run_time_average(args: argparse.Namespace) -> None:
    """Run time averaging workflow.
    
    Args:
        args: Parsed command line arguments
    """
    try:
        output_files = time_average_from_config(args.config)
        print(f"\nTime averaging completed successfully!")
        print(f"Output files created: {len(output_files)}")
        for file_path in output_files:
            print(f"  {file_path}")
    except Exception as e:
        print(f"Error during time averaging: {e}")
        sys.exit(1)


def main(argv: Optional[list] = None) -> None:
    """Main CLI entry point.
    
    Args:
        argv: Optional command line arguments (for testing)
    """
    parser = create_parser()
    args = parser.parse_args(argv)
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        if args.command == 'perfect-model-obs':
            # Use shared function to avoid duplication
            run_perfect_model_obs_from_args(args)
            
        elif args.command == 'time-average':
            run_time_average(args)
            
        else:
            print(f"Unknown command: {args.command}")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
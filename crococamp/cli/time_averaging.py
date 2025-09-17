"""Time averaging CLI for CrocoCamp MOM6 data processing."""

import argparse
import sys
from typing import Optional

from ..io.mom6_time_averager import time_average_from_config


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser for time averaging."""
    
    parser = argparse.ArgumentParser(
        description='CrocoCamp Time Averaging - Temporal resampling tool for MOM6 NetCDF files',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        'config',
        type=str,
        help="Path to time-averaging configuration YAML file"
    )
    
    return parser


def main(argv: Optional[list] = None) -> None:
    """Main CLI entry point for time averaging.
    
    Args:
        argv: Optional command line arguments (for testing)
    """
    parser = create_parser()
    args = parser.parse_args(argv)
    
    try:
        output_files = time_average_from_config(args.config)
        print(f"\nTime averaging completed successfully!")
        print(f"Output files created: {len(output_files)}")
        for file_path in output_files:
            print(f"  {file_path}")
    except Exception as e:
        print(f"Error during time averaging: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
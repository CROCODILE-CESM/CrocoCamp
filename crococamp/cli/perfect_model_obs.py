"""Command-line interface for perfect model observation workflows."""

import argparse
import sys

from ..utils.cli import add_perfect_model_obs_arguments, run_perfect_model_obs_from_args


def main() -> None:
    """Main CLI entry point for perfect model observation processing."""
    
    parser = argparse.ArgumentParser(
        description='Script to call perfect_model_obs on multiple model and obs_seq.in files.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Use shared argument functions to avoid duplication
    add_perfect_model_obs_arguments(parser)

    args = parser.parse_args()
    
    try:
        # Use shared function to avoid duplication
        run_perfect_model_obs_from_args(args)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

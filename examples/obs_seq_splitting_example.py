"""Example usage of obs_seq_splitting utilities."""

from datetime import timedelta
import pandas as pd
from crococamp.utils.obs_seq_splitting import (
    parse_averaging_period,
    compute_time_windows,
    select_files_for_windows,
    generate_window_filename
)

def example_usage():
    """Demonstrate basic usage of obs_seq_splitting functions."""
    
    print("=== obs_seq_splitting Example Usage ===\n")
    
    # Example 1: Parse averaging periods
    print("1. Parsing averaging periods:")
    periods = [
        "monthly", 
        "7D", 
        {"days": 30, "hours": 12},
        "2W"
    ]
    
    for period_spec in periods:
        parsed = parse_averaging_period(period_spec)
        print(f"   {period_spec} -> {parsed}")
    print()
    
    # Example 2: Time window computation
    print("2. Computing time windows:")
    t0 = pd.Timestamp("2023-01-01")
    t1 = pd.Timestamp("2023-03-01") 
    period = timedelta(days=30)
    
    windows = compute_time_windows(t0, t1, period)
    print(f"   Time span: {t0} to {t1}")
    print(f"   Period: {period}")
    print(f"   Generated {len(windows)} windows:")
    for i, (start, end) in enumerate(windows):
        print(f"     Window {i}: {start} to {end}")
    print()
    
    # Example 3: File selection
    print("3. File selection for windows:")
    # Mock obs_seq time map
    obs_time_map = {
        "obs_seq_file1.in": (pd.Timestamp("2023-01-15"), pd.Timestamp("2023-02-15")),
        "obs_seq_file2.in": (pd.Timestamp("2023-01-25"), pd.Timestamp("2023-02-25")),
        "obs_seq_file3.in": (pd.Timestamp("2023-02-10"), pd.Timestamp("2023-03-10")),
    }
    
    window_files = select_files_for_windows(obs_time_map, windows)
    
    for j, files in window_files.items():
        window_start, window_end = windows[j]
        print(f"   Window {j} ({window_start} to {window_end}): {len(files)} files")
        for filename in files:
            obs_start, obs_end = obs_time_map[filename]
            print(f"     - {filename} ({obs_start} to {obs_end})")
    print()
    
    # Example 4: Filename generation
    print("4. Generated filenames:")
    for i, (start, end) in enumerate(windows):
        filename = generate_window_filename(start, end)
        print(f"   Window {i}: {filename}")
    
    print("\n=== Example completed successfully ===")


if __name__ == "__main__":
    example_usage()
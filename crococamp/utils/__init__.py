"""CrocoCamp utilities package."""

from .obs_seq_splitting import (
    parse_averaging_period,
    get_model_time_bounds,
    build_obs_seq_time_map,
    compute_time_windows,
    select_files_for_windows,
    split_obs_seq_files
)

__all__ = [
    "parse_averaging_period",
    "get_model_time_bounds", 
    "build_obs_seq_time_map",
    "compute_time_windows",
    "select_files_for_windows",
    "split_obs_seq_files"
]
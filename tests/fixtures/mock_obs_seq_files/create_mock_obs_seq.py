"""Helper module to create mock obs_seq.in files for testing.

This module provides utilities to generate valid obs_seq.in files
with configurable observations for testing purposes.
"""

from pathlib import Path
from typing import List, Tuple


def create_obs_seq_in(
    filepath: Path,
    observations: List[Tuple[float, float, float, float, int]]
) -> None:
    """Create a valid obs_seq.in file with specified observations.
    
    Args:
        filepath: Path where the obs_seq.in file will be created
        observations: List of tuples (lon_rad, lat_rad, depth_m, value, obs_type)
                     where lon_rad and lat_rad are in radians,
                     depth_m is depth in meters (negative or positive),
                     value is the observation value,
                     obs_type is observation type ID (e.g., 12 for ARGO_TEMPERATURE)
    """
    num_obs = len(observations)
    
    header = f""" obs_sequence
obs_type_definitions
          4
          11 TEMPERATURE
          12 ARGO_TEMPERATURE
          13 SALINITY
          14 ARGO_SALINITY
num_copies:       1 num_qc:               1
num_obs:       {num_obs} max_num_obs:       {num_obs}
CROCOLAKE observation
CROCOLAKE QC
first:            1 last:         {num_obs}
"""
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(header)
        
        for i, (lon_rad, lat_rad, depth_m, value, obs_type) in enumerate(observations, start=1):
            prev_idx = i - 1 if i > 1 else -1
            next_idx = i + 1 if i < num_obs else -1
            
            obs_block = f"""OBS        {i}
{value}
1
{prev_idx}          {next_idx}          -1
obdef
loc3d
{lon_rad}   {lat_rad}   {abs(depth_m)}   3
kind
{obs_type}
986 147232
0.0020000000949949026
"""
            f.write(obs_block)


def degrees_to_radians(degrees: float) -> float:
    """Convert degrees to radians."""
    import math
    return degrees * math.pi / 180.0

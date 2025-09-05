"""Observation sequence handling utilities for CrocoCamp workflows."""

import numpy as np
import pydartdiags.obs_sequence.obs_sequence as obsq
from shapely.vectorized import contains


def trim_obs_seq_in(obs_in_file, hull_polygon, hull_points, trimmed_obs_file):
    """
    trim obs_seq.in file to preserve only observation within specificed geographical area

    Arguments:
    obs_in_file  -- obs_seq.in file to trim (full path)
    hull_polygon -- shapely Polygon object representing the convex hull
    hull_points -- numpy array of hull vertices for display
    trimmed_obs_file -- filename of trimmed observations file
    """

    # Get bounding box for print
    lon_min, lat_min = hull_points.min(axis=0)
    lon_max, lat_max = hull_points.max(axis=0)
    print("   Convex hull boundaries:")
    print(f"   Longitude: {lon_min:.2f}° to {lon_max:.2f}° (0-360 convention)")
    print(f"   Latitude: {lat_min:.2f}° to {lat_max:.2f}°")

    obs_seq_in = obsq.ObsSequence(obs_in_file)
    obs_lon = obs_seq_in.df['longitude'].values
    obs_lat = obs_seq_in.df['latitude'].values

    print(f"\n   Number of observations before filtering to convex hull: {len(obs_seq_in.df)}")

    # Create a mask for observations inside the convex hull
    within_hull_mask = contains(hull_polygon, obs_lon, obs_lat)
    if not np.any(within_hull_mask):
        raise ValueError("No observations found within the convex hull.")

    obs_seq_in_original_df = obs_seq_in.df.copy()
    obs_seq_in.df = obs_seq_in.df[within_hull_mask].reset_index(drop=True)
    print(f"\n   Number of observations after filtering to convex hull: {len(obs_seq_in.df)}")
    print(f"   Percentage of original observations retained: {len(obs_seq_in.df)/len(obs_seq_in_original_df)*100:.1f}%")

    obs_seq_in.write_obs_seq(trimmed_obs_file)
    print(f"   Trimmed file stored to {trimmed_obs_file}.")



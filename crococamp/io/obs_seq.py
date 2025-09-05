"""Observation sequence handling utilities for CrocoCamp workflows."""

import glob
import os
import shutil

import dask.dataframe as dd
import numpy as np
import pandas as pd
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


def merge_pair_to_parquet(perf_obs_file, orig_obs_file, parquet_path):

    # Read obs_sequence files
    perf_obs_out = obsq.ObsSequence(perf_obs_file)
    perf_obs_out.update_attributes_from_df()
    trimmed = obsq.ObsSequence(orig_obs_file)
    trimmed.update_attributes_from_df()

    obs_col = [col for col in trimmed.df.columns.to_list() if col.endswith("_observation") or col=="observation"]
    print(trimmed.df.columns.to_list())
    print(obs_col)
    if len(obs_col) > 1:
        raise ValueError("More than one observation columns found.")
    else:
        trimmed.df = trimmed.df.rename(columns={obs_col[0]:"obs"})
    perf_obs_out.df = perf_obs_out.df.rename(columns={"truth":"model"})

    # Generate unique hash for merging (obs_num is not unique across files, but
    # time is)
    def compute_hash(df, cols, hash_col="hash"):
        concat = df[cols].astype(str).agg('-'.join, axis=1)
        df[hash_col] = pd.util.hash_pandas_object(concat, index=False).astype('int64')
        return df

    trimmed.df = compute_hash(trimmed.df, ['obs_num', 'seconds', 'days'])
    perf_obs_out.df = compute_hash(perf_obs_out.df, ['obs_num', 'seconds', 'days'])

    # Merge DataFrames
    merge_key = "hash"
    trimmed.df = trimmed.df.set_index(merge_key, drop=True)
    perf_obs_out.df = perf_obs_out.df.set_index(merge_key, drop=True)
    ref_cols = ['longitude', 'latitude', 'time', 'vertical', 'type', 'obs_err_var']
    merged = pd.merge(
        trimmed.df[ref_cols + ['obs']],
        perf_obs_out.df[ref_cols + ['model']],
        left_index=True,
        right_index=True,
        how='outer',
        suffixes=('_trim', '_perf')
    )

    # Check that reference columns are indeed identical and deduplicate them
    for col in ref_cols:
        c_trim, c_perf = f"{col}_trim", f"{col}_perf"
        if c_trim in merged and c_perf in merged:
            if merged[c_trim].equals(merged[c_perf]) or np.all(np.isclose(merged[c_trim], merged[c_perf], atol=1e-13)):
                merged = merged.drop(columns=[c_trim])
                merged = merged.rename(columns={c_perf: col})
            else:
                raise ValueError(f"{col}: {c_trim} and {c_perf} not identical. The two files probably do not refer to the same observation space.")
        else:
            raise ValueError(f"{col}: one of {c_trim}, {c_perf} not present in merged DataFrame. The two files probably do not refer to the same observation space.")

    # Sort dataframe by time -> position -> depth
    sort_order = ['time', 'longitude', 'latitude', 'vertical']
    merged = merged.sort_values(by=sort_order)

    # Add diagnostic columns
    merged['residual'] = merged['obs'] - merged['model']
    merged['abs_residual'] = np.abs(merged['residual'])
    merged['normalized_residual'] = merged['residual'] / np.sqrt(merged['obs_err_var'])
    merged['squared_residual'] = merged['residual'] ** 2
    merged['log_likelihood'] = -0.5 * (
        merged['residual'] ** 2 / merged['obs_err_var'] +
        np.log(2 * np.pi * merged['obs_err_var'])
    )

    # Reorder columns
    column_order = [
        'time', 'longitude', 'latitude', 'vertical', 'type',
        'model', 'obs', 'obs_err_var',
    ]
    remaining_cols = [col for col in merged.columns if col not in column_order]
    merged = merged[column_order + remaining_cols]

    ddf = dd.from_pandas(merged)
    name_function = lambda x: f"tmp-model-obs-{x}.parquet"
    append = True
    if not os.listdir(parquet_path):
        append=False # create new dataset if it's the first in the folder,
                     # append otherwise
    ddf.to_parquet(
        parquet_path,
        append=append,
        name_function=name_function,
        write_metadata_file=True,
        ignore_divisions=True
    )

    return


def merge_model_obs_to_parquet(config, trim_obs):
    output_folder = config['output_folder']
    parquet_folder = config['parquet_folder']
    if trim_obs:
        obs_folder = config['trimmed_obs_folder']
    else:
        obs_folder = config['obs_in_folder']

    from ..utils.config import check_or_create_folder
    print("Validating parquet_folder...")
    check_or_create_folder(parquet_folder, "parquet_folder")

    perf_obs_files = sorted(
        glob.glob(os.path.join(output_folder, "obs_seq*.out"))
    )
    orig_obs_files = sorted(
        glob.glob(os.path.join(obs_folder, "*obs_seq*.in"))
    )
    print("perf_obs_files")
    print(perf_obs_files)
    print("orig_obs_files")
    print(orig_obs_files)

    tmp_parquet_folder = os.path.join(parquet_folder, "tmp")
    os.makedirs(tmp_parquet_folder, exist_ok=True)

    for perf_obs_f, orig_obs_f in zip(perf_obs_files, orig_obs_files):
        merge_pair_to_parquet(perf_obs_f, orig_obs_f, tmp_parquet_folder)

    ddf = dd.read_parquet(tmp_parquet_folder)
    ddf = ddf.repartition(partition_size="300MB")
    name_function = lambda x: f"model-obs-{x}.parquet"
    ddf.to_parquet(
        parquet_folder,
        append=False,
        name_function=name_function
    )

    shutil.rmtree(tmp_parquet_folder)

    return

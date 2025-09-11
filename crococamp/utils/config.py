"""Configuration utilities for CrocoCamp workflows."""

from datetime import timedelta
import os
from typing import Any, Dict, List
import yaml


def resolve_path(path: str, config_file: str) -> str:
    """Resolve path to absolute, using config_file location as base for relative paths."""
    if os.path.isabs(path):
        return path
    config_dir = os.path.dirname(os.path.abspath(config_file))
    return os.path.abspath(os.path.join(config_dir, path))


def read_config(config_file: str) -> Dict[str, Any]:
    """Read configuration from YAML file."""
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Config file '{config_file}' does not exist")

    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        for key in config:
            if isinstance(config[key], str):
                config[key] = resolve_path(config[key], config_file)

        config["input_nml"] = os.path.join(config['perfect_model_obs_dir'], "input.nml")
        config = convert_time_window(config)
        return config
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML file: {e}")

def validate_config_keys(config: Dict[str, Any], required_keys: List[str]) -> None:
    """Validate that all required keys are present in config."""
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise KeyError(f"Required keys missing from config: {missing_keys}")

def convert_time_window(config: Dict[str, Any]) -> Dict[str, Any]:
    """Validate time window for perfect_model_obs"""

    # Assuming 7 in week, 30 days in month, 365 in year
    days_in_week = 7
    days_in_month = 30
    days_in_year = 365

    keys = ["years", "months", "weeks", "days", "hours", "minutes", "seconds"]
    time_window_dict = config.get("time_window", None)
    for key in keys:
        if key not in time_window_dict:
            time_window_dict[key] = 0

    years = time_window_dict["years"]
    months = time_window_dict["months"]
    weeks = time_window_dict["weeks"]
    days = time_window_dict["days"]
    hours = time_window_dict["hours"]
    minutes = time_window_dict["minutes"]
    seconds = time_window_dict["seconds"]

    time_window = timedelta(
        days=days+weeks*days_in_week+months*days_in_month+years*days_in_year,
        hours=hours,
        minutes=minutes,
        seconds=seconds
    )

    config["time_window"] = {}
    config["time_window"]["days"] = time_window.days
    config["time_window"]["seconds"] = time_window.seconds

    return config

def check_directory_not_empty(dir_path: str, name: str) -> None:
    """Check if directory exists and is not empty."""
    if not os.path.isdir(dir_path):
        raise NotADirectoryError(f"{name} '{dir_path}' does not exist or is not a directory")

    if not os.listdir(dir_path):
        raise ValueError(f"{name} '{dir_path}' is empty")

def check_nc_files_only(dir_path: str, name: str) -> None:
    """Check if directory contains only .nc files."""
    all_files = [f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))]

    if not all_files:
        raise ValueError(f"{name} '{dir_path}' does not contain any files")

    nc_files = [f for f in all_files if f.endswith('.nc')]

    if len(nc_files) != len(all_files):
        non_nc_files = [f for f in all_files if not f.endswith('.nc')]
        raise ValueError(f"{name} '{dir_path}' contains non-.nc files: {non_nc_files}")

    if not nc_files:
        raise ValueError(f"{name} '{dir_path}' does not contain any .nc files")

def check_nc_file(file_path: str, name: str) -> None:
    """Check if file exists and has .nc extension."""
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"{name} '{file_path}' does not exist")

    if not file_path.endswith('.nc'):
        raise ValueError(f"{name} '{file_path}' is not a .nc file")

def check_or_create_folder(output_folder: str, name: str) -> None:
    """Check if folder exists, if not, create it."""
    if os.path.exists(output_folder):
        if not os.path.isdir(output_folder):
            raise NotADirectoryError(f"{name} '{output_folder}' exists but is not a directory")
        if os.listdir(output_folder):
            raise ValueError(f"{name} '{output_folder}' exists but is not empty")
    else:
        try:
            os.makedirs(output_folder, exist_ok=True)
        except OSError as e:
            raise OSError(f"Could not create {name} '{output_folder}': {e}")

def clear_folder(folder_path: str) -> None:
    """Clear content at folder_path."""
    import shutil

    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.remove(file_path)
                print(f'Deleted file: {file_path}')
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')

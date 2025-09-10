"""Configuration utilities for CrocoCamp workflows."""

import os
import yaml


def resolve_path(path, config_file):
    """Resolve path to absolute, using config_file location as base for relative paths."""
    if os.path.isabs(path):
        return path
    config_dir = os.path.dirname(os.path.abspath(config_file))
    return os.path.abspath(os.path.join(config_dir, path))

def read_config(config_file):
    """Read configuration from YAML file."""
    if not os.path.exists(config_file):
        raise FileNotFoundError(f"Config file '{config_file}' does not exist")

    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        for key in config:
            if isinstance(config[key], str):
                config[key] = resolve_path(config[key], config_file)

        return config
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML file: {e}")


def validate_config_keys(config, required_keys):
    """Validate that all required keys are present in config."""
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise KeyError(f"Required keys missing from config: {missing_keys}")


def check_directory_not_empty(dir_path, name):
    """Check if directory exists and is not empty."""
    if not os.path.isdir(dir_path):
        raise NotADirectoryError(f"{name} '{dir_path}' does not exist or is not a directory")

    if not os.listdir(dir_path):
        raise ValueError(f"{name} '{dir_path}' is empty")


def check_nc_files_only(dir_path, name):
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


def check_nc_file(file_path, name):
    """Check if file exists and has .nc extension."""
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"{name} '{file_path}' does not exist")

    if not file_path.endswith('.nc'):
        raise ValueError(f"{name} '{file_path}' is not a .nc file")


def check_or_create_folder(output_folder, name):
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

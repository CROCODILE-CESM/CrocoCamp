"""Namelist file utilities for CrocoCamp workflows."""

import os
import re
import tempfile
from typing import Any, Union


def read_namelist(file_path: str) -> str:
    """Read namelist file and return as string."""
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"Namelist file '{file_path}' does not exist")

    try:
        with open(file_path, 'r') as f:
            return f.read()
    except IOError as e:
        raise IOError(f"Could not read namelist file '{file_path}': {e}")


def write_namelist(file_path: str, content: str) -> None:
    """Write content to namelist file."""
    try:
        with open(file_path, 'w') as f:
            f.write(content)
    except IOError as e:
        raise IOError(f"Could not write namelist file '{file_path}': {e}")


def symlink_to_namelist(input_nml: str) -> None:
    """Create a symbolic link to a namelist file."""
    if not os.path.isfile(input_nml):
        raise FileNotFoundError(f"Source namelist file '{input_nml}' does not exist")

    try:
        dest = os.path.join(os.getcwd(), "input.nml")
        if dest == input_nml:
            raise ValueError("Source and destination for symlink are the same.")
        if os.path.islink(dest):
            os.remove(dest)
            print(f"Symlink '{dest}' removed.")
        elif os.path.exists(dest):
            raise ValueError(f"'{dest}' exists and is not a symlink. Not removing nor continuing execution.")
        os.symlink(input_nml, dest)
        print(f"Symlink {dest} -> '{input_nml}' created.")
    except OSError as e:
        raise OSError(f"Could not create symlink from '{input_nml}' to '{dest}': {e}")

def cleanup_namelist_symlink() -> None:
    """Remove the symbolic link to the namelist file."""
    dest = os.path.join(os.getcwd(), "input.nml")
    try:
        if os.path.islink(dest):
            os.remove(dest)
            print(f"Symlink '{dest}' removed.")
        elif os.path.exists(dest):
            print(f"'{dest}' exists but is not a symlink. Not removing.")
        else:
            print(f"No symlink '{dest}' found to remove.")
    except OSError as e:
        raise OSError(f"Could not remove symlink '{dest}': {e}")

def update_namelist_param(content: str, section: str, param: str, value: Union[str, int, float, bool], string: bool = True) -> str:
    """Update a parameter in a namelist section."""
    section_pattern = f'&{section}'

    lines = content.split('\n')
    in_section = False
    updated = False

    for j, line in enumerate(lines):
        if line.strip().startswith(section_pattern):
            in_section = True
            continue

        if in_section and line.strip().startswith('&') and not line.strip().startswith(section_pattern):
            in_section = False
            continue

        if in_section:
            param_pattern = rf'^\s*{re.escape(param)}\s*='
            if re.match(param_pattern, line):
                if string:
                    lines[j] = f'   {param.ljust(27)}= "{value}",'
                else:
                    lines[j] = f'   {param.ljust(27)}= {value},'
                updated = True
                break

    if not updated:
        raise ValueError(f"Parameter '{param}' not found in section '&{section}'")

    return '\n'.join(lines)

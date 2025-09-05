"""Namelist file utilities for CrocoCamp workflows."""

import os
import re


def read_namelist(file_path):
    """Read namelist file and return as string."""
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"Namelist file '{file_path}' does not exist")

    try:
        with open(file_path, 'r') as f:
            return f.read()
    except IOError as e:
        raise IOError(f"Could not read namelist file '{file_path}': {e}")


def write_namelist(file_path, content):
    """Write content to namelist file."""
    try:
        with open(file_path, 'w') as f:
            f.write(content)
    except IOError as e:
        raise IOError(f"Could not write namelist file '{file_path}': {e}")


def update_namelist_param(content, section, param, value, string=True):
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

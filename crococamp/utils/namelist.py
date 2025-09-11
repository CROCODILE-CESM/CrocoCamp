"""Namelist file utilities for CrocoCamp workflows."""

import os
import re
import shutil
import tempfile
from typing import Any, Union

class Namelist():
    """Class to handle file operations related to perfect_model_obs input.nml
    namelist file.

    It includes methods to read, write, and update parameters, as well as
    generating necessary symlink for perfect_model_obs to execute correctly.
    """

    def __init__(self, namelist_path: str) -> None:
        """Initialize Namelist with path to namelist file.

        Arguments:
        namelist_path: Path to the namelist file
        """

        print("Setting up symlink for input.nml...")
        self.namelist_path = namelist_path
        self.symlink_to_namelist()

        # Create backup and read namelist
        shutil.copy2(self.namelist_path, "input.nml.backup")
        print("Created backup: input.nml.backup")

        self.content = self.read_namelist()

    def read_namelist(self) -> str:
        """Read namelist file and return as string."""
        if not os.path.isfile(self.namelist_path):
            raise FileNotFoundError(f"Namelist file '{self.namelist_path}' does not exist")

        try:
            with open(self.namelist_path, 'r') as f:
                return f.read()
        except IOError as e:
            raise IOError(f"Could not read namelist file '{self.namelist_path}': {e}")

    def write_namelist(self, namelist_path : str = None, content : str = None) -> None:
        """Write content to namelist file."""
        if namelist_path is None:
            namelist_path = self.namelist_path
        if content is None:
            content = self.content

        try:
            with open(namelist_path, 'w') as f:
                f.write(content)
        except IOError as e:
            raise IOError(f"Could not write namelist file '{namelist_path}': {e}")

    def symlink_to_namelist(self) -> None:
        """Create a symbolic link to a namelist file."""
        if not os.path.isfile(self.namelist_path):
            raise FileNotFoundError(f"Source namelist file '{self.namelist_path}' does not exist")

        try:
            self.dest = os.path.join(os.getcwd(), "input.nml")
            if self.dest == self.namelist_path:
                raise ValueError("Source and self.destination for symlink are the same.")
            if os.path.islink(self.dest):
                os.remove(self.dest)
                print(f"Symlink '{self.dest}' removed.")
            elif os.path.exists(self.dest):
                raise ValueError(f"'{self.dest}' exists and is not a symlink. Not removing nor continuing execution.")
            os.symlink(self.namelist_path, self.dest)
            print(f"Symlink {self.dest} -> '{self.namelist_path}' created.")
        except OSError as e:
            raise OSError(f"Could not create symlink from '{self.namelist_path}' to '{self.dest}': {e}")

    def cleanup_namelist_symlink(self) -> None:
        """Remove the symbolic link to the namelist file."""
        try:
            if os.path.islink(self.dest):
                os.remove(self.dest)
                print(f"Symlink '{self.dest}' removed.")
            elif os.path.exists(self.dest):
                print(f"'{self.dest}' exists but is not a symlink. Not removing.")
            else:
                print(f"No symlink '{self.dest}' found to remove.")
        except OSError as e:
            raise OSError(f"Could not remove symlink '{self.dest}': {e}")

    def update_namelist_param(self, section: str, param: str, value: Union[str, int, float, bool], string: bool = True) -> None:
        """Update a parameter in a namelist section.

        Arguments:
        section: Namelist section (without initial '&')
        param: Parameter name to update
        value: New value for the parameter
        string: Whether the value is a string (True) or a number (False)
                (default: True)
        """

        section_pattern = f'&{section}'

        lines = self.content.split('\n')
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

        self.content = '\n'.join(lines)

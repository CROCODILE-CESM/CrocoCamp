"""Namelist file utilities for CrocoCamp workflows."""

import os
import re
import shutil
from typing import Union, List

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
            with open(self.namelist_path, 'r', encoding='utf-8') as f:
                return f.read()
        except IOError as e:
            raise IOError(f"Could not read namelist file '{self.namelist_path}': {e}") from e

    def write_namelist(self, namelist_path : str = None, content : str = None) -> None:
        """Write content to namelist file."""
        if namelist_path is None:
            namelist_path = self.namelist_path
        if content is None:
            content = self.content

        try:
            with open(namelist_path, 'w', encoding='utf-8') as f:
                f.write(content)
        except IOError as e:
            raise IOError(f"Could not write namelist file '{namelist_path}': {e}") from e

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
            raise OSError(f"Could not create symlink from "
                         f"'{self.namelist_path}' to '{self.dest}': {e}") from e

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
            raise OSError(f"Could not remove symlink '{self.dest}': {e}") from e

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

    def update_obs_kind_nml(self, obs_types_list: List[str]) -> None:
        """Update assimilate_these_obs_types parameter in obs_kind_nml section.

        Args:
            obs_types_list: List of observation types to assimilate

        The formatting follows the pattern:
        assimilate_these_obs_types = 'FIRST_TYPE'
                                     'SECOND_TYPE'
                                     'THIRD_TYPE'
        All entries are left-aligned starting after the '=' sign.
        """
        if not obs_types_list:
            raise ValueError("obs_types_list cannot be empty")

        section_name = 'obs_kind_nml'
        param_name = 'assimilate_these_obs_types'

        lines = self.content.split('\n')
        param_start_line, param_end_line = self._find_parameter_location(
            lines, section_name, param_name)

        indent = '   '  # Standard indentation
        new_lines = self._format_obs_types_parameter(param_name, obs_types_list, indent)

        if param_start_line is None:
            # Parameter doesn't exist, need to add it before the section end
            section_end = self._find_section_end(lines, section_name)
            if section_end is None:
                raise ValueError(f"Could not find end of section '&{section_name}'")
            lines[section_end:section_end] = new_lines
        else:
            # Parameter exists, replace it
            lines[param_start_line:param_end_line + 1] = new_lines

        self.content = '\n'.join(lines)

    def _find_parameter_location(self, lines: List[str], section_name: str,
                               param_name: str) -> tuple:
        """Find the start and end line numbers of a parameter in a section."""
        in_section = False
        param_start_line = None
        param_end_line = None

        for i, line in enumerate(lines):
            stripped = line.strip()

            if stripped.startswith(f'&{section_name}'):
                in_section = True
                continue

            if in_section and stripped.startswith('&') and not stripped.startswith(f'&{section_name}'):
                in_section = False
                continue

            if in_section and stripped == '/':
                in_section = False
                continue

            if in_section and re.match(rf'^\s*{re.escape(param_name)}\s*=', line):
                param_start_line = i
                # Find the end of this parameter
                for j in range(i + 1, len(lines)):
                    next_line = lines[j].strip()
                    # If we hit another parameter, section end, or empty line followed by parameter
                    if (re.match(r'^\s*\w+\s*=', lines[j]) or
                        next_line == '/' or
                        next_line.startswith('&')):
                        param_end_line = j - 1
                        break

                # If we didn't find an end, it goes to end of section
                if param_end_line is None:
                    for j in range(i + 1, len(lines)):
                        if lines[j].strip() == '/' or lines[j].strip().startswith('&'):
                            param_end_line = j - 1
                            break
                    if param_end_line is None:
                        param_end_line = len(lines) - 1
                break

        return param_start_line, param_end_line

    def _find_section_end(self, lines: List[str], section_name: str) -> int:
        """Find the end line number of a section."""
        in_section = False
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith(f'&{section_name}'):
                in_section = True
                continue
            if in_section and stripped == '/':
                return i
        return None

    def _format_obs_types_parameter(self, param_name: str, obs_types_list: List[str],
                                   indent: str) -> List[str]:
        """Format the obs_types parameter with proper Fortran namelist continuation syntax.

        Args:
            param_name: Name of the parameter
            obs_types_list: List of observation types
            indent: Base indentation string

        Returns:
            List of formatted lines
        """
        lines = []

        if not obs_types_list:
            lines.append(f"{indent}{param_name.ljust(27)}= ''")
            return lines

        # First line with parameter name and first value
        first_line = f"{indent}{param_name.ljust(27)}= '{obs_types_list[0]}'"
        lines.append(first_line)

        # Continuation lines for remaining values - align with the opening quote
        continuation_indent = ' ' * (len(indent) + 27 + 3)  # align after '= ' at position 27+3
        for obs_type in obs_types_list[1:]:
            lines.append(f"{continuation_indent}'{obs_type}'")

        return lines

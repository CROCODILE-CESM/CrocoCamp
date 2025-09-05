#!/bin/bash

# CrocoCamp Python package skeleton

mkdir -p crococamp/utils
mkdir -p crococamp/io
mkdir -p crococamp/workflows
mkdir -p crococamp/cli

touch crococamp/__init__.py
touch crococamp/utils/__init__.py
touch crococamp/io/__init__.py
touch crococamp/workflows/__init__.py
touch crococamp/cli/__init__.py

touch crococamp/utils/config.py
touch crococamp/utils/namelist.py
touch crococamp/io/file_utils.py
touch crococamp/io/model_grid.py
touch crococamp/io/obs_seq.py
touch crococamp/workflows/model_obs.py
touch crococamp/cli/crococamp_cli.py

# General files
touch README.md
touch .gitignore
touch requirements.txt
touch pyproject.toml

echo "# CrocoCamp" > README.md
echo "See pyproject.toml for metadata and dependencies." > README.md

echo "*.pyc" > .gitignore
echo "__pycache__/" >> .gitignore
echo "*.pkl" >> .gitignore
echo "*.parquet" >> .gitignore
echo ".ipynb_checkpoints/" >> .gitignore
echo "sample_data/" >> .gitignore

echo "# Example requirements file" > requirements.txt
echo "xarray" >> requirements.txt
echo "dask" >> requirements.txt
echo "pandas" >> requirements.txt

cat << EOF > pyproject.toml
[project]
name = "crococamp"
version = "0.1.0"
description = "Tools for comparing ocean models, observations, and gridded products."
authors = [{ name="Enrico Milanese", email="enrico.milanese@whoi.edu" }]
readme = "README.md"
license = { file="LICENSE" }
requires-python = ">=3.8"
dependencies = [
    "xarray",
    "dask",
    "pandas",
    "pyyaml",
    "shapely",
    "scipy",
    "xesmf",
    "xgcm"
]

[build-system]
requires = ["setuptools>=61.0", "wheel"]
build-backend = "setuptools.build_meta"
EOF

echo "CrocoCamp project structure with general files created!"

#!/usr/bin/env bash

set -euo pipefail

# flag to execute downloads for tutorials
TUTORIAL=0
for arg in "$@"; do
    if [[ "$arg" == "--tutorial" ]]; then
        TUTORIAL=1
    fi
done

## create conda environment with name set in envpaths.sh
source ./envpaths.sh
mamba env create --name "$CONDA_ENV_NAME" -f ../environment.yml -y
CONDA_ENV_PATH=$(conda env list | awk -v env="$CONDA_ENV_NAME" '$1 == env { print $NF }')

## scripts to run when environment is activated
mkdir -p $CONDA_ENV_PATH/etc/conda/activate.d

# load environmental paths
CONDA_SCRIPTS_PATH=$CONDA_ENV_PATH/etc/conda/activate.d/
cp ./envpaths.sh $CONDA_SCRIPTS_PATH
echo "source \"${CONDA_SCRIPTS_PATH}envpaths.sh\"" > $CONDA_ENV_PATH/etc/conda/activate.d/load_paths.sh
chmod +x $CONDA_ENV_PATH/etc/conda/activate.d/load_paths.sh

if [[ "$TUTORIAL" -eq 1 ]]; then
    ./tutorials_download.sh
fi

#!/usr/bin/env sh
export DART_ROOT_PATH="/glade/u/home/emilanese/work/DART-Casper/"

# Define conda environment name
export CONDA_ENV_NAME="crococamp-dev"

#### DO NOT MODIFY BELOW THIS LINE ####
# Set up paths
export CROCOLAKE_OBS_CONV_PATH=${DART_ROOT_PATH%/}/observations/obs_converters/CrocoLake/
export PYTHONPATH="$CROCOLAKE_OBS_CONV_PATH:\$PYTHONPATH"

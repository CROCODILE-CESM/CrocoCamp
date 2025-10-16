#!/usr/bin/env sh
module load conda
CONDA_ENV_NAME="crococamp-2025"
mamba env create --name "$CONDA_ENV_NAME" -f environment.yml -y
CONDA_ENV_PATH=$(conda env list | awk -v env="$CONDA_ENV_NAME" '$1 == env { print $NF }')
mkdir -p $CONDA_ENV_PATH/etc/conda/activate.d
echo 'module load nco' > $CONDA_ENV_PATH/etc/conda/activate.d/load_modules.sh
echo 'export PYTHONPATH="/glade/u/home/emilanese/work/DART-Casper/observations/obs_converters/CrocoLake:\$PYTHONPATH"' > $CONDA_ENV_PATH/etc/conda/activate.d/set_pythonpath.sh
chmod +x $CONDA_ENV_PATH/etc/conda/activate.d/load_modules.sh
chmod +x $CONDA_ENV_PATH/etc/conda/activate.d/set_pythonpath.sh

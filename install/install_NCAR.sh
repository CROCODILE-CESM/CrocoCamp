#!/usr/bin/env sh
case "$HOSTNAME" in
    *hpc.ucar.edu*) # verify that user is on NCAR's HPC
        ## create conda environment with name set in envpaths.sh
        source ./envpaths_NCAR.sh
        mamba env create --name "$CONDA_ENV_NAME" -f ../environment.yml -y
        CONDA_ENV_PATH=$(conda env list | awk -v env="$CONDA_ENV_NAME" '$1 == env { print $NF }')

        ## scripts to run when environment is activated
        mkdir -p $CONDA_ENV_PATH/etc/conda/activate.d

        # load environmental paths
        cp ./envpaths.sh $CONDA_ENV_PATH/etc/conda/activate.d/
        echo 'source ./envpaths.sh' > $CONDA_ENV_PATH/etc/conda/activate.d/set_paths.sh
        chmod +x $CONDA_ENV_PATH/etc/conda/activate.d/load_modules.sh
        chmod +x $CONDA_ENV_PATH/etc/conda/activate.d/load_paths.sh
        ;;
    *)
        echo "Interrupting installation: you are not on NCAR's Casper or Derecho, look for install.sh or other install files."
esac

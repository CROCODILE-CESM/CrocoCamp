# CrocoCamp

**CrocoCamp** is a Python toolset for harmonizing and comparing ocean model outputs and observation datasets. It streamlines workflows for interpolating model data into the observation space, producing tabular data in Parquet format ready for analysis and interactive visualization.

## Features

Current:
- Batch processing of model and observation files
- Generation of diagnostic and comparison files in Parquet format
- Robust YAML configuration and command-line interface
- Designed for extensibility and reproducibility
- Ocean models supported: MOM6
- Ocean observation format supported: DART obs_seq.in format
- **Modular architecture** with clean separation of concerns

Future:
- Automated regridding of model grids when comparing different model resolutions or different ocean models (e.g. ROMS and MOM6)
- Automated regridding when comparing models to gridded products (e.g. GLORYS)

## Architecture

The toolkit is organized into logical modules:

- **`utils/`** - Configuration and namelist file utilities
- **`io/`** - File handling, model grids, and observation sequence processing
- **`workflows/`** - High-level workflow orchestration
- **`cli/`** - Command-line interfaces

## Installation

### Prerequisites and Dependencies

#### 1. Install DART for MOM6
DART (Data Assimilation Research Testbed) is required to run the `perfect_model_obs` executable, which interpolates MOM6 ocean model output onto the observation space provided in obs_seq.in format. The following instructions are for Linux machines.

```bash
git clone git@github.com:CROCODILE-CESM/DART.git
cd DART
git checkout mom6-scripting
cd build_templates
cp mkmf.template.intel.linux mkmf.template
cd ../models/MOM6/work
./quickbuild.sh
```

For installation on other operating systems or more detailed information, see the [DART documentation](https://docs.dart.ucar.edu/).

#### 2. Create conda environment
```bash
mamba create --name crococamp python=3.12
conda activate crococamp
```

#### 3. Install pyDARTdiags
```bash
git clone git@github.com:NCAR/pyDARTdiags.git
cd pyDARTdiags
pip install .
```

#### 4. Install CrocoCamp
```bash
git clone git@github.com:CROCODILE-CESM/CrocoCamp.git
cd CrocoCamp
pip install .
```

#### 5. Load NCAR modules (if on NCAR systems)
```bash
module load nco
```

## Usage

### Command Line Interface

Process model-observation pairs using the main CLI:

```bash
# Basic usage
perfect-model-obs -c config.yaml

# With observation trimming to model grid boundaries
perfect-model-obs -c config.yaml --trim

# Skip time matching (assumes 1:1 file correspondence)
perfect-model-obs -c config.yaml --no-matching

# Convert existing outputs to parquet only
perfect-model-obs -c config.yaml --parquet-only
```

### Programmatic Usage (Class-based API)

For Python scripts and Jupyter notebooks, use the class-based API:

```python
from crococamp.workflows import WorkflowModelObs

# Load workflow from configuration file
workflow = WorkflowModelObs.from_config_file("config.yaml")

# Or create workflow with config dictionary directly in code
config = {
    'model_files_folder': '/path/to/model/files',
    'obs_seq_in_folder': '/path/to/obs_seq_in/files', 
    'output_folder': '/path/to/output',
    'template_file': '/path/to/template.nc',
    'static_file': '/path/to/static.nc',
    'ocean_geometry': '/path/to/geometry.nc',
    'perfect_model_obs_dir': '/path/to/perfect_model_obs',
    'parquet_folder': '/path/to/parquet'
}
workflow = WorkflowModelObs(config)

# Run the complete workflow
files_processed = workflow.run(trim_obs=True, no_matching=False)

# Or run specific steps
files_processed = workflow.process_files(trim_obs=True)
workflow.merge_model_obs_to_parquet(trim_obs=True)
```

You can also override configuration values:

```python
# Override config values when creating workflow
workflow = WorkflowModelObs.from_config_file(
    "config.yaml", 
    output_folder="/custom/output/path",
    trim_obs=True
)

# Or modify configuration after creation
workflow.set_config("parquet_folder", "/custom/parquet/path")

# Access configuration values
output_folder = workflow.get_config("output_folder") 
workflow.print_config()  # Print all current configuration

# Get required configuration keys for validation
required_keys = workflow.get_required_config_keys()

workflow.run()
```

### Legacy Function-based API

For backward compatibility, the original function-based API is still available:

```python
from crococamp.workflows.model_obs import process_files, merge_model_obs_to_parquet
from crococamp.utils.config import read_config

config = read_config("config.yaml")
files_processed = process_files(config, trim_obs=True)
merge_model_obs_to_parquet(config, trim_obs=True)
```

## Demo

### Quick Start with Demo Data

To run the demo and get familiar with CrocoCamp:

```bash
perfect-model-obs -c ./demo/config.yaml -t
```

This command uses the `-c` flag to specify the configuration file (`./demo/config.yaml`) and the `-t` flag to enable observation trimming to model grid boundaries.

This command will:
- Process demo model and observation files
- Apply observation trimming to model grid boundaries (`-t` flag)
- Generate output files in multiple folders:
  - `demo/out_obs_seq_in/` - Contains obs_seq*.out files with perfect model observations (model data reinterpolated onto observation space)
  - `demo/out_trimmed_obs_seq_in/` - Contains observation files trimmed to model grid boundaries
  - `demo/out_parquet/` - Contains merged model-observation data in Parquet format with diagnostics
  - `demo/input_bckp/` - Contains backup copies of DART input.nml configuration files

### Exploring Results

After running the demo, you can explore the results using the provided Jupyter notebook in `examples/model-obs-comparison.ipynb`.
- This notebook demonstrates how to load and visualize the parquet datasets generated by `perfect-model-obs`
- It provides examples of comparing model and observation data, including diagnostic values such as:
  - `residual` (obs - model)
  - `abs_residual` (absolute residual)
  - `normalized_residual` (residual normalized by observation error)
  - `squared_residual` (squared residual)
  - `log_likelihood` (log-likelihood of model-observation fit)

## Configuration

Edit the provided `configs/config.yaml` to set your input, output, and model/obs paths:

```yaml
model_in_folder: /path/to/model/files/
obs_in_folder: /path/to/obs/files/
output_folder: /path/to/output/
template_file: /path/to/template.nc
static_file: /path/to/static.nc
ocean_geometry: /path/to/ocean_geometry.nc
```

## Examples

See the `examples/` and `configs/` folders for notebooks and reference configurations, including:
- `examples/model-obs-comparison.ipynb` - Model-observation comparison examples
- `configs/config.yaml` - Example configuration file
- `configs/input.nml` - Example DART namelist file

---

**For more details, see the full documentation or open an issue.**

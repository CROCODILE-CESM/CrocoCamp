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
- **`viz/`** - Interactive visualization widgets for data analysis

## Key Classes and Functions

CrocoCamp provides several key classes and functions that are demonstrated in the Jupyter notebooks:

### Workflow Classes

- **`WorkflowModelObs`** - Main workflow class for model-observation comparison. Orchestrates the entire pipeline from data processing to parquet generation. Supports configuration from YAML files and provides methods like `run()`, `process_files()`, and `merge_model_obs_to_parquet()`.

### Visualization Classes

- **`InteractiveWidgetMap`** - Interactive geographical map widget for visualizing model-observation comparisons spatially. Provides time-series controls, variable selection, and statistical overlays.

- **`InteractiveWidgetProfile`** - Interactive profile visualization widget for analyzing vertical data structure (e.g., temperature/salinity vs depth). Features dropdown axis selection, observation type filtering, and automatic y-axis inversion for ocean profiles.

### Configuration Classes

- **`MapConfig`** - Configuration class for customizing map widget appearance, including colormap, figure size, plot title, and map extent.

- **`ProfileConfig`** - Configuration class for customizing profile widget settings, including figure size, marker properties, axis inversion, and grid display.

### Utility Functions

- **`read_config()`** - Loads and validates YAML configuration files, resolving relative paths and setting defaults.
- **`Namelist`** - Class for handling DART namelist file operations, including reading, writing, and updating parameters.

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

## Observation Types Configuration

CrocoCamp now supports automatic configuration of observation types for DART assimilation through the `use_these_obs` field in your configuration file. This feature reads observation type definitions from DART's `obs_def_ocean_mod.rst` file and automatically updates the `input.nml` file's `&obs_kind_nml` section.

### Basic Usage

Add the `use_these_obs` field to your `config.yaml`:

```yaml
# Basic observation types
use_these_obs:
  - FLOAT_TEMPERATURE
  - FLOAT_SALINITY
  - CTD_TEMPERATURE
  - CTD_SALINITY
```

### ALL_<FIELD> Syntax

You can use the `ALL_<FIELD>` syntax to automatically include all observation types for a specific quantity:

```yaml
use_these_obs:
  - ALL_TEMPERATURE    # Includes all temperature-related obs types
  - ALL_SALINITY       # Includes all salinity-related obs types  
  - SATELLITE_SSH      # Include specific additional types
```

### Supported Field Types

The `ALL_<FIELD>` syntax supports any quantity type defined in DART's obs_def_ocean_mod.rst:
- `ALL_TEMPERATURE` - All temperature observation types (FLOAT_TEMPERATURE, CTD_TEMPERATURE, XBT_TEMPERATURE, etc.)
- `ALL_SALINITY` - All salinity observation types (FLOAT_SALINITY, CTD_SALINITY, etc.)
- `ALL_U_CURRENT_COMPONENT` - All U-velocity observation types
- `ALL_V_CURRENT_COMPONENT` - All V-velocity observation types
- `ALL_SEA_SURFACE_HEIGHT` - All sea surface height observation types
- See DART documentation for a complete list

### Example Configuration

```yaml
perfect_model_obs_dir: /path/to/DART/models/MOM6/work
model_files_folder: /path/to/model/files
obs_seq_in_folder: /path/to/obs_seq_files
output_folder: /path/to/output

# Observation types configuration
use_these_obs:
  - ALL_TEMPERATURE      # Expands to ~15 temperature obs types
  - FLOAT_SALINITY       # Specific salinity type
  - SATELLITE_SSH        # Sea surface height from satellites

# Other configuration...
time_window:
  days: 5
  hours: 0
```

### How It Works

1. **Parsing**: CrocoCamp reads the DART observation definitions from:
   `{perfect_model_obs_dir}/../../../observations/forward_operators/obs_def_ocean_mod.rst`

2. **Validation**: Each observation type in your `use_these_obs` list is validated against the available types

3. **Expansion**: `ALL_<FIELD>` entries are expanded to include all observation types with matching quantity (`QTY_<FIELD>`)

4. **Namelist Update**: The `input.nml` file's `&obs_kind_nml` section is automatically updated with proper Fortran formatting:

```fortran
&obs_kind_nml
   assimilate_these_obs_types = 'ARGO_TEMPERATURE'
                                'BOTTLE_TEMPERATURE'
                                'CTD_TEMPERATURE'
                                'FLOAT_SALINITY'
                                'SATELLITE_SSH'
   evaluate_these_obs_types   = ''
   /
```

### Error Handling

If an observation type is invalid or a field expansion fails, CrocoCamp will show a warning and continue with the existing `input.nml` configuration:

```
Warning: Could not process observation types: Invalid observation type 'INVALID_TYPE'
Continuing with existing obs_kind_nml configuration
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

After running the demo, you can explore the results using the provided Jupyter notebooks:
- `examples/demo-model-obs-comparison.ipynb` demonstrates the `InteractiveWidgetMap` class for interactive geographical visualization of model-observation comparisons
- `examples/demo-interactive-profile.ipynb` demonstrates the `InteractiveWidgetProfile` class for interactive profile analysis (temperature/salinity vs depth)

These notebooks provide examples of comparing model and observation data, including diagnostic values such as:
  - `residual` (obs - model)
  - `abs_residual` (absolute residual)
  - `normalized_residual` (residual normalized by observation error)
  - `squared_residual` (squared residual)
  - `log_likelihood` (log-likelihood of model-observation fit)

## Configuration

Edit the provided `configs/config.yaml` to set your input, output, and model/obs paths. For detailed configuration options and explanations, see `configs/config_template.yaml` which provides a comprehensive template with extensive comments explaining each parameter's purpose and usage:

```yaml
model_files_folder: /path/to/model/files/
obs_seq_in_folder: /path/to/obs/files/
output_folder: /path/to/output/
parquet_folder: /path/to/parquet/
template_file: /path/to/template.nc
static_file: /path/to/static.nc
ocean_geometry: /path/to/ocean_geometry.nc
perfect_model_obs_dir: /path/to/DART/models/MOM6/work
```

## Examples

See the `examples/` and `configs/` folders for notebooks and reference configurations, including:
- `examples/demo-model-obs-comparison.ipynb` - Interactive map widget demonstration for model-observation comparison
- `examples/demo-interactive-profile.ipynb` - Interactive profile widget demonstration for vertical data analysis
- `configs/config.yaml` - Example configuration file
- `configs/config_template.yaml` - Comprehensive configuration template with detailed comments
- `configs/input.nml` - Example DART namelist file

---

**For more details, see the full documentation or open an issue.**

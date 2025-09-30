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

### Workflow Classes

**`WorkflowModelObs`** - Main workflow class for model-observation comparisons
- `from_config_file(config_file)` - Create workflow from YAML configuration file
- `run()` - Execute complete workflow 
- `process_files()` - Process model and observation files
- `merge_model_obs_to_parquet()` - Convert results to parquet format
- `get_config(key)` - Get configuration value
- `set_config(key, value)` - Set configuration value
- `print_config()` - Print current configuration

### Visualization Classes

Both widgets below support both pandas and dask DataFrames.

**`InteractiveWidgetMap`** - Interactive map widget for spatial data visualization
- Constructor: `InteractiveWidgetMap(dataframe, config=None)`
- `setup()` - Initialize and display the interactive map widget
- Provides dropdowns for selecting plot variables, observation types, and time filtering

**`MapConfig`** - Configuration class for map widget customization
- Parameters: `colormap`, `figure_size`, `scatter_size`, `map_extent`, etc.

**`InteractiveWidgetProfile`** - Interactive profile widget for vertical profile analysis  
- Constructor: `InteractiveWidgetProfile(dataframe, x='obs', y='vertical', config=None)`
- `setup()` - Initialize and display the interactive profile widget
- Supports custom x and y axis selections for profile analysis
- Ideal for analyzing Argo float or CTD profile comparisons

**`ProfileConfig`** - Configuration class for profile widget customization  
- Parameters: `figure_size`, `marker_size`, `invert_yaxis`, etc.

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



## Getting Started

The best way to learn CrocoCamp is through the hands-on tutorials in the `tutorials/` folder. These Jupyter notebooks guide you through:

**Tutorial 1** (`tutorial1_MOM6-CL-comparison.ipynb`): 
- Setting up a basic model-observation comparison workflow
- Using MOM6 ocean model output and World Ocean Database observations
- Visualizing results with the interactive map widget

**Tutorial 2** (`tutorial2_MOM6-CL-comparison-float.ipynb`):
- Generating custom observation files from CrocoLake 
- Analyzing single Argo float profiles
- Using the interactive profile widget for vertical profile comparisons

These tutorials demonstrate:
- Loading and configuring workflows with `WorkflowModelObs`
- Running the complete processing pipeline
- Exploring results including diagnostic values such as:
  - `residual` (obs - model)
  - `abs_residual` (absolute residual)
  - `normalized_residual` (residual normalized by observation error)
  - `squared_residual` (squared residual)
  - `log_likelihood` (log-likelihood of model-observation fit)

## Configuration

Edit the provided `configs/config_template.yaml` to set your input, output, and model/obs paths. The template file contains all necessary configuration options with detailed comments explaining each parameter for educational purposes:

```yaml
model_files_folder: /path/to/model/files/
obs_seq_in_folder: /path/to/obs/files/
output_folder: /path/to/output/
template_file: /path/to/template.nc
static_file: /path/to/static.nc
ocean_geometry: /path/to/ocean_geometry.nc
```

**Note for NCAR HPC Users:** The paths in the provided configuration files (`config_tutorial_1.yaml`, `config_tutorial_2.yaml`) and some paths used in the tutorial notebooks are pre-configured for resources available on NCAR's High Performance Computing systems, including:
- CrocoLake observation dataset paths
- DART tools paths 
- Pre-compiled `perfect_model_obs` executable locations

These paths may need to be adjusted if running on other systems. Note that DART needs to be compiled separately on Derecho and Casper, so we provide two pre-compiled paths for the workshop:
- Derecho: `/glade/u/home/emilanese/work/CROCODILE-DART-fork/models/MOM6/work`
- Casper: `/glade/u/home/emilanese/work/DART-Casper/models/MOM6/work`

## Examples

See the `tutorials/` and `configs/` folders for notebooks and reference configurations, including:
- `tutorials/tutorial1_MOM6-CL-comparison.ipynb` - Model-observation comparison with interactive map widget examples  
- `tutorials/tutorial2_MOM6-CL-comparison-float.ipynb` - Model-observation comparison with interactive profile widget examples
- `configs/config_template.yaml` - Template configuration file with detailed comments
- `configs/config_tutorial_1.yaml` - Configuration for Tutorial 1
- `configs/config_tutorial_2.yaml` - Configuration for Tutorial 2

---

**For more details, see the full documentation or open an issue.**

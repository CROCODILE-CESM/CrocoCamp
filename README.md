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

```bash
git clone https://github.com/CROCODILE-CESM/CrocoCamp.git
cd CrocoCamp
pip install -e .
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
- `examples/model-obs-comparison-kate.ipynb` - Model-observation comparison examples
- `examples/regridding_20250716.ipynb` - Model-model comparison workflow
- `configs/config.yaml` - Example configuration file
- `configs/input.nml` - Example DART namelist file
- `regridding_20250716.ipynb` - Model-model comparison workflow
- `config.yaml` - Example configuration file

---

**For more details, see the full documentation or open an issue.**

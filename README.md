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

Future:
- Automated regridding of model grids when comparing different model resolutions or different ocean models (e.g. ROMS and MOM6)
- Automated regridding of when comparing models to gridded products (e.g. GLORYS)

## Installation

```bash
git clone https://github.com/your-org/CrocoCamp.git
cd CrocoCamp
pip install -e .
```

## Usage

TODO

## Configuration

Edit the provided `config.yaml` to set your input, output, and model/obs paths.

## Example

See the `examples/` folder for notebooks and typical workflows.

---

**For more details, see the full documentation or open an issue.**

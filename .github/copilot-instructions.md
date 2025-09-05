# Copilot Instructions for CrocoCamp (`ghca` branch)

Welcome, Copilot!  
This repository is for **CrocoCamp**, a Python toolset for harmonizing and comparing ocean model outputs and observation datasets.  
**You are to work only on the `ghca` branch.**

---

## Project Context

CrocoCamp is designed to streamline workflows for comparing, regridding, and evaluating ocean model outputs against observations and other gridded data products.  
The repository will support **three main workflows**:

1. **Model vs. Observational Data**  
   - Compare model output to point observations (e.g., DART obs_seq.in).  
   - This workflow is currently implemented in `perfect_model_obs_split_loop.py`.

2. **Model vs. Model Data**  
   - Compare outputs from different ocean models, possibly with different grids or resolutions.

3. **Model vs. Gridded Products**  
   - Compare model outputs to external gridded products (e.g., GLORYS, reanalysis datasets).

**We are starting with workflow (1), then will expand to (2) and (3).**  
Longer term, tools for **binning and averaging data prior to interpolation** into the target space (e.g., model into obs space) will also be added.

---

## General Guidelines

- **Language & Style:**  
  - Use idiomatic Python (3.8+).  
  - Prefer clear NumPy or Google-style docstrings.  
  - Write modular code with clear separation of concerns.

- **Structure & Naming:**  
  - All code is under `crococamp/` with logical submodules (`io`, `utils`, `cli`, `workflows`).
  - Use CamelCase for classes, snake_case for functions/variables.
  - Keep the public API minimal and clean.

- **Dependencies:**  
  - Use only those listed in `pyproject.toml` and `requirements.txt`.
  - Propose/add new dependencies only when necessary and document why in PRs.

---

## Command-Line Interface

- CLI entry points should live in `crococamp/cli/crococamp_cli.py`.
- Existing scripts like `ref_files/perfect_model_obs_split_loop.py` should eventually be wrapped as CLI commands.

---

## Protype references

- Files in `ref_files/` contains current prototypes of the available functionalities.
- Use files in `ref_files/` as references for the workflows
- Use the jupyter notebook `ref_files/model-obs-comparison-kate.ipynb` as references for the example to visualiza model-obs comparison
- Use the jupyter notebook `ref_files/regridding_20250716.ipynb` as reference for the model-model comparison workflow
- `ref_files/input.nml` is an example of input namelist file that is used by DART's perfect_model_obs script when called by `ref_files/perfect_model_obs_split_loop.py`
- `ref_files/config.yaml` is the current configuration file example

## Documentation

- Keep `README.md` updated with new features and usage.
- All user-facing functions/scripts must have clear docstrings.
- If adding new configuration options, update any relevant YAML examples.

---

## Testing & Validation

- Prefer pure Python and in-memory tests; avoid committing large data files.
- New features should include at least one minimal test or usage example (in code, docstring, or README).

---

## File and Directory Conventions

- Do **not** commit generated data files (e.g., `.parquet`, `.pkl`) or large outputs.
- Respect and update `.gitignore` as needed.

---

## Branch-Specific Instructions

- **Work only on the `ghca` branch.**
- Do not open PRs against `main` unless explicitly instructed.
- Comment or document any changes that are experimental or meant for future refactoring.

---

## Communication

- Prioritize clarity and maintainability over cleverness.
- If a design decision is not obvious, leave a code comment explaining your reasoning.

---

Thank you for helping make CrocoCamp robust and future-proof!


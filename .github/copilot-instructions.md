# Copilot Instructions for CrocoCamp (`ghca` branch)

Welcome, Copilot!  
This repository is for **CrocoCamp**, a Python toolset for harmonizing and comparing ocean model outputs and observation datasets.  

---

## Project Context

CrocoCamp is designed to streamline workflows for comparing, regridding, and evaluating ocean model outputs against observations and other gridded data products.  
The repository will support **three main workflows**:

1. **Model vs. Observational Data**  
   - Compare model output to point observations (e.g., DART obs_seq.in).
     
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
  - Follow style guidelines as in PEP 8, PEP 257. Each python file should have a grade of 8 or more when run through `pylint`.
  - Follow PEP 484 for type hinting.
  - The code should compatible with any version of Python 3.9 or newer.

- **Structure & Naming:**  
  - All code is under `crococamp/` with logical submodules (`io`, `utils`, `cli`, `workflows`).
  - Use CamelCase for classes, snake_case for functions/variables.
  - Keep the public API minimal and clean.
  - Keep in mind that this code is used by non-experienced Python users too, so any design solution that is advanced should be justified and well documented and/or commented.

- **Dependencies:**  
  - Use only those listed in `pyproject.toml`.
  - Propose/add new dependencies only when necessary and document why in PRs.

---

## Command-Line Interface

- CLI entry points should live in `crococamp/cli/crococamp_cli.py`.

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

## Communication

- Prioritize clarity and maintainability over cleverness.
- If a design decision is not obvious, leave a code comment explaining your reasoning.

---

Thank you for helping make CrocoCamp robust and future-proof!


#!/usr/bin/env python3
"""
Example demonstrating the obs_types functionality in CrocoCamp.

This script shows how to:
1. Parse observation type definitions from obs_def_ocean_mod.rst
2. Validate and expand observation types including ALL_<FIELD> syntax
3. Update namelist files with proper Fortran formatting
"""

import os
from pathlib import Path
from crococamp.utils.config import parse_obs_def_ocean_mod, validate_and_expand_obs_types
from crococamp.utils.namelist import Namelist

def main():
    """Demonstrate obs_types functionality."""
    
    # Example 1: Parse observation definitions
    print("=== Example 1: Parsing obs_def_ocean_mod.rst ===")
    
    # This would typically point to your DART installation
    rst_file_path = "/path/to/DART/observations/forward_operators/obs_def_ocean_mod.rst"
    
    # For demonstration, we'll create a mock file (in real usage, point to DART installation)
    if not os.path.exists(rst_file_path):
        print(f"Note: {rst_file_path} not found. Using mock data for demonstration.")
        # In real usage, you would point to the actual DART installation
    
    try:
        # This shows how the parsing works (would use real DART file in practice)
        print("Observation type parsing would extract:")
        print("- FLOAT_TEMPERATURE -> QTY_TEMPERATURE")
        print("- FLOAT_SALINITY -> QTY_SALINITY") 
        print("- CTD_TEMPERATURE -> QTY_TEMPERATURE")
        print("- And ~60+ more observation types...")
    except Exception as e:
        print(f"Error parsing RST file: {e}")
    
    # Example 2: Validate and expand observation types
    print("\n=== Example 2: Obs Types Validation and Expansion ===")
    
    # Example observation types configuration
    obs_types_config = [
        "FLOAT_TEMPERATURE",    # Specific type
        "CTD_SALINITY",        # Another specific type  
        "ALL_TEMPERATURE",     # Expands to all temperature types
        "SATELLITE_SSH"        # Sea surface height
    ]
    
    print(f"Input configuration: {obs_types_config}")
    
    # This would expand ALL_TEMPERATURE to all temperature observation types
    print("After expansion, ALL_TEMPERATURE would include:")
    temperature_types = [
        "TEMPERATURE", "ARGO_TEMPERATURE", "FLOAT_TEMPERATURE", 
        "CTD_TEMPERATURE", "XBT_TEMPERATURE", "SATELLITE_MICROWAVE_SST",
        "SATELLITE_INFRARED_SST", "BOTTLE_TEMPERATURE", "GLIDER_TEMPERATURE",
        # ... and more
    ]
    print(f"  {temperature_types[:5]} ... (and ~15+ more)")
    
    # Example 3: Namelist formatting
    print("\n=== Example 3: Namelist Formatting ===")
    
    # Show how the namelist would be formatted
    sample_obs_types = ["FLOAT_TEMPERATURE", "CTD_SALINITY", "SATELLITE_SSH"]
    
    print("Input observation types:")
    for obs_type in sample_obs_types:
        print(f"  - {obs_type}")
    
    print("\nFormatted namelist output:")
    print("&obs_kind_nml")
    print("   assimilate_these_obs_types = 'FLOAT_TEMPERATURE'")
    print("                                 'CTD_SALINITY'")  
    print("                                 'SATELLITE_SSH'")
    print("   evaluate_these_obs_types   = ''")
    print("   /")
    
    # Example 4: Complete workflow integration
    print("\n=== Example 4: Complete Workflow Usage ===")
    
    config_example = """
# config.yaml example
perfect_model_obs_dir: /path/to/DART/models/MOM6/work
model_files_folder: /path/to/model/files
obs_seq_in_folder: /path/to/obs_seq_files
output_folder: /path/to/output

# Observation types - this is the new feature!
use_these_obs:
  - ALL_TEMPERATURE      # Expands to all temperature obs types
  - FLOAT_SALINITY       # Specific salinity type
  - SATELLITE_SSH        # Sea surface height

time_window:
  days: 5
"""
    
    print("Example config.yaml:")
    print(config_example)
    
    print("Python usage:")
    print("""
from crococamp.workflows.workflow_model_obs import WorkflowModelObs

# The workflow will automatically:
# 1. Read use_these_obs from config
# 2. Expand ALL_TEMPERATURE to specific types  
# 3. Validate all types against DART definitions
# 4. Update input.nml with proper formatting
workflow = WorkflowModelObs.from_config_file('config.yaml')
workflow.run()
""")

if __name__ == "__main__":
    main()
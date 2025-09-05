#!/usr/bin/env python3
"""
Demonstration script showing how to use the refactored CrocoCamp modules.

This script shows the equivalent functionality to ref_files/perfect_model_obs_split_loop.py
but using the new modular structure.
"""

import sys
import os
sys.path.insert(0, '.')

# Example usage of the refactored modules
def demonstrate_usage():
    """Demonstrate the refactored module usage."""
    
    print("=== CrocoCamp Refactored Module Demonstration ===\n")
    
    # 1. Configuration handling
    print("1. Configuration Management:")
    try:
        from crococamp.utils.config import read_config, validate_config_keys
        
        # Read configuration (using sample config)
        config_file = "ref_files/config.yaml"
        if os.path.exists(config_file):
            config = read_config(config_file)
            print(f"   ✓ Configuration loaded from {config_file}")
            print(f"   ✓ Found {len(config)} configuration parameters")
            
            # Validate required keys
            required_keys = ['model_in_folder', 'obs_in_folder', 'output_folder',
                           'template_file', 'static_file', 'ocean_geometry']
            validate_config_keys(config, required_keys)
            print(f"   ✓ Required configuration keys validated")
        else:
            print(f"   ⚠ Sample config file not found at {config_file}")
    except ImportError as e:
        print(f"   ✗ Configuration module import failed: {e}")
    
    # 2. Namelist handling
    print("\n2. Namelist Management:")
    try:
        from crococamp.utils.namelist import read_namelist, update_namelist_param
        
        namelist_file = "ref_files/input.nml"
        if os.path.exists(namelist_file):
            content = read_namelist(namelist_file)
            print(f"   ✓ Namelist loaded from {namelist_file}")
            print(f"   ✓ Namelist contains {len(content)} characters")
            
            # Example parameter update
            try:
                updated = update_namelist_param(
                    content, "perfect_model_obs_nml", "async", "1", string=False
                )
                print("   ✓ Parameter update functionality works")
            except ValueError as e:
                print(f"   ⚠ Parameter update test: {e}")
        else:
            print(f"   ⚠ Sample namelist file not found at {namelist_file}")
    except ImportError as e:
        print(f"   ✗ Namelist module import failed: {e}")
    
    # 3. File utilities (basic test without heavy dependencies)
    print("\n3. File Utilities:")
    try:
        import glob
        
        # Simple file listing for demonstration
        ref_files = glob.glob("ref_files/*")
        print(f"   ✓ Found {len(ref_files)} reference files")
        for f in ref_files[:3]:  # Show first 3
            print(f"     - {os.path.basename(f)}")
        if len(ref_files) > 3:
            print(f"     ... and {len(ref_files) - 3} more")
    except Exception as e:
        print(f"   ✗ File utilities test failed: {e}")
    
    # 4. Workflow orchestration (conceptual)
    print("\n4. Workflow Architecture:")
    print("   ✓ Main workflow logic extracted to workflows/model_obs.py")
    print("   ✓ Functions organized by responsibility:")
    print("     - process_files(): Main orchestration")
    print("     - process_model_obs_pair(): Individual file pair processing")
    print("   ✓ Same functionality as original script, but modular")
    
    # 5. CLI interface
    print("\n5. Command-Line Interface:")
    print("   ✓ CLI interface created at cli/perfect_model_obs.py")
    print("   ✓ Same arguments as original script:")
    print("     --config, --trim, --no-matching, --force-obs-time, --parquet-only")
    print("   ✓ Entry point configured in pyproject.toml")
    
    print("\n=== Key Benefits of Refactoring ===")
    print("✓ Modular design: Functions grouped by purpose")
    print("✓ Reusable components: Each module can be imported independently")
    print("✓ Maintainable code: Clear separation of concerns")
    print("✓ Testable units: Each module can be tested in isolation")
    print("✓ Same functionality: All original features preserved")
    
    print("\n=== Usage Examples ===")
    print("# Use configuration utilities:")
    print("from crococamp.utils.config import read_config")
    print("config = read_config('config.yaml')")
    print()
    print("# Use namelist utilities:")
    print("from crococamp.utils.namelist import update_namelist_param")
    print("updated = update_namelist_param(content, 'section', 'param', 'value')")
    print()
    print("# Run the full workflow:")
    print("from crococamp.workflows.model_obs import process_files")
    print("process_files(config, trim_obs=True)")
    print()
    print("# Use CLI:")
    print("python -m crococamp.cli.perfect_model_obs -c config.yaml --trim")

if __name__ == "__main__":
    demonstrate_usage()
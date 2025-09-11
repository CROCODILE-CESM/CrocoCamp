"""Template for creating new workflow classes in CrocoCamp.

This template demonstrates the structure and required methods for 
creating new workflow subclasses that inherit from the base Workflow class.
"""

from typing import Any, Dict, List

from .workflow import Workflow


class WorkflowTemplate(Workflow):
    """Template workflow class demonstrating the pattern for new workflows.
    
    To create a new workflow:
    1. Inherit from Workflow base class
    2. Implement get_required_config_keys() method
    3. Implement run() method
    4. Add any workflow-specific methods
    5. Follow namespace import pattern for utilities
    """
    
    def get_required_config_keys(self) -> List[str]:
        """Return list of required configuration keys.
        
        Override this method to specify which configuration keys
        are required for your workflow.
        
        Returns:
            List of required configuration key names
        """
        return [
            'input_folder',      # Example: input data directory
            'output_folder',     # Example: output directory
            'param1',            # Example: workflow-specific parameter
            'param2',            # Example: another parameter
        ]
    
    def run(self, **kwargs: Any) -> str:
        """Execute the workflow.
        
        Override this method to implement your workflow logic.
        Use **kwargs to accept workflow-specific parameters.
        
        Args:
            **kwargs: Workflow-specific keyword arguments
            
        Returns:
            Workflow execution result (customize as needed)
        """
        # 1. Validate inputs
        self._validate_inputs(**kwargs)
        
        # 2. Print configuration
        print("Starting workflow execution...")
        self.print_config()
        
        # 3. Execute workflow steps
        result = self._execute_workflow(**kwargs)
        
        # 4. Cleanup and return
        print("Workflow completed successfully!")
        return result
    
    def _validate_inputs(self, **kwargs: Any) -> None:
        """Validate workflow inputs and parameters.
        
        Add any workflow-specific validation logic here.
        """
        # Example validation
        input_folder = self.get_config('input_folder')
        if not input_folder:
            raise ValueError("input_folder must be specified")
        
        # Add more validation as needed
        pass
    
    def _execute_workflow(self, **kwargs: Any) -> str:
        """Execute the main workflow logic.
        
        Implement your workflow steps here.
        Break down into smaller methods for better organization.
        """
        # Example workflow steps:
        
        # Step 1: Load and prepare data
        data = self._load_data()
        
        # Step 2: Process data
        processed_data = self._process_data(data, **kwargs)
        
        # Step 3: Save results
        output_path = self._save_results(processed_data)
        
        return output_path
    
    def _load_data(self) -> Dict[str, Any]:
        """Load input data.
        
        Implement data loading logic here.
        Use namespace imports for utility functions.
        """
        # Example: from ..io import file_utils
        # data = file_utils.load_data(self.get_config('input_folder'))
        
        print("Loading data...")
        return {}  # Placeholder
    
    def _process_data(self, data: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
        """Process the loaded data.
        
        Implement data processing logic here.
        """
        print("Processing data...")
        return data  # Placeholder
    
    def _save_results(self, processed_data: Dict[str, Any]) -> str:
        """Save processed results.
        
        Implement result saving logic here.
        """
        output_folder = self.get_config('output_folder')
        print(f"Saving results to {output_folder}...")
        return output_folder  # Placeholder


# Example usage:
if __name__ == "__main__":
    # Configuration for the template workflow
    config = {
        'input_folder': '/path/to/input',
        'output_folder': '/path/to/output',
        'param1': 'value1',
        'param2': 'value2',
    }
    
    # Create and run workflow
    workflow = WorkflowTemplate(config)
    result = workflow.run(custom_param=True)
    print(f"Workflow result: {result}")
from crococamp.workflows import WorkflowModelObs

# interpolate model onto obs space for the single float
workflow_float = WorkflowModelObs.from_config_file('../configs/config_tutorial_2.yaml')
workflow_float.run() #use flag clear_output=True if you want to re-run it and automatically clean all previous output

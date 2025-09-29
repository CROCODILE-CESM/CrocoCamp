from crococamp.workflows import WorkflowModelObs

# interpolate model onto obs space
workflow_wod = WorkflowModelObs.from_config_file('../configs/config_tutorial_2.yaml')
workflow_wod.run() #use flag clear_output=True if you want to re-run it and automatically clean all previous output

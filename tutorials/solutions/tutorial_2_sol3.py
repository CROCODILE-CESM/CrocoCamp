from crococamp.viz import InteractiveWidgetMap

# Create an interactive map widget to visualize model-observation comparisons
# The widget provides controls for selecting variables, observation types, and time ranges
widget = InteractiveWidgetMap(df)
widget.setup()

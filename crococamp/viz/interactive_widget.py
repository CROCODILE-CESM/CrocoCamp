"""Base class for interactive visualization widgets."""

from abc import ABC, abstractmethod
from typing import Union

import dask.dataframe as dd
import ipywidgets as widgets
import pandas as pd
from IPython.display import display, clear_output


class InteractiveWidget(ABC):
    """Abstract base class for interactive visualization widgets.

    This class provides common functionality for visualization widgets including:
    - Pandas/Dask DataFrame handling
    - Widget lifecycle management
    - Abstract methods for widget-specific implementation
    """

    def __init__(self, dataframe: Union[pd.DataFrame, dd.DataFrame], config=None):
        """Initialize the interactive widget.

        Args:
            dataframe: Input dataframe (pandas or dask) containing data
            config: Configuration instance for customization (optional)
        """
        self.df = dataframe
        self.config = config
        self.output = None

    def _setup_widget_workflow(self):
        """Execute the standard widget setup workflow."""
        self._initialize_state()
        self._create_widgets()
        self._setup_callbacks()

    @abstractmethod
    def _initialize_state(self) -> None:
        """Initialize widget-specific state variables."""

    @abstractmethod
    def _create_widgets(self) -> None:
        """Create all UI widgets specific to this visualization type."""

    @abstractmethod
    def _setup_callbacks(self) -> None:
        """Set up widget observers for interactive updates."""

    @abstractmethod
    def _plot(self) -> None:
        """Create the visualization plot."""

    def _is_dask_dataframe(self) -> bool:
        """Check if the dataframe is a dask DataFrame."""
        return hasattr(self.df, 'compute')

    def _compute_if_needed(
        self, series_or_df: Union[pd.Series, pd.DataFrame, dd.Series, dd.DataFrame]
    ) -> Union[pd.Series, pd.DataFrame]:
        """Compute dask series/dataframe if needed, otherwise return as-is."""
        if hasattr(series_or_df, 'compute'):
            return series_or_df.compute()
        return series_or_df

    def _persist_if_needed(
        self, df: Union[pd.DataFrame, dd.DataFrame]
    ) -> Union[pd.DataFrame, dd.DataFrame]:
        """Persist dask dataframe if needed, otherwise return as-is."""
        if hasattr(df, 'persist'):
            return df.persist()
        return df

    def clear(self) -> None:
        """Clear the visualization output.

        This method clears the current visualization so users can call setup()
        again with different parameters without having both visualizations displayed.
        """
        if self.output:
            with self.output:
                clear_output(wait=True)

    def setup(self) -> widgets.Widget:
        """Initialize the widget with default selections and display.

        Returns:
            The widget box containing all controls and output
        """
        # Initialize with defaults
        self._initialize_widget()

        # Create and display widget layout
        widget_box = self._create_widget_layout()
        #display(widget_box)

        # Initial plot
        self._plot()

        return widget_box

    @abstractmethod
    def _initialize_widget(self) -> None:
        """Initialize widget state for display."""

    @abstractmethod
    def _create_widget_layout(self) -> widgets.Widget:
        """Create the widget layout for display."""

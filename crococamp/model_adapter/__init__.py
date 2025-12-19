"""ModelAdapter modules for CrocoCamp."""

from .model_adapter import ModelAdapter
from .model_adapter import ModelAdapterCapabilities
from .model_adapter_MOM6 import ModelAdapterMOM6
from .model_adapter_ROMS import ModelAdapterROMS

__all__ = [
    "ModelAdapter",
    "ModelAdapterMOM6",
    "ModelAdapterROMS",
    "ModelAdapterCapabilities"
]

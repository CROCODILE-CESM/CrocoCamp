from __future__ import annotations

from typing import Type

from .model_adapter import ModelAdapter
from .model_adapter_MOM6 import ModelAdapterMOM6

_ADAPTERS: dict[str, Type[BaseModelAdapter]] = {
    "mom6": ModelAdapterMOM6,
}

def create_model_adapter(ocean_model: str, **kwargs) -> BaseModelAdapter:
    if ocean_model is None:
        raise ValueError("ocean_model is required (e.g. 'MOM6' or 'ROMS').")

    key = ocean_model.strip().lower()
    try:
        adapter_cls = _ADAPTERS[key]
    except KeyError as e:
        allowed = ", ".join(sorted(_ADAPTERS.keys()))
        raise ValueError(
            f"Unknown ocean_model={ocean_model!r}. Allowed values: {allowed}."
        ) from e

    return adapter_cls(**kwargs)

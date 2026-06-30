from __future__ import annotations

from .base import CompanyAdapter
from .flat_catalog import FlatCatalogAdapter
from .gainr import GainrAdapter


ADAPTERS: dict[str, type[CompanyAdapter]] = {
    GainrAdapter.name: GainrAdapter,
    FlatCatalogAdapter.name: FlatCatalogAdapter,
}


def get_adapter(name: str) -> CompanyAdapter:
    try:
        adapter_type = ADAPTERS[name]
    except KeyError as exc:
        available = ", ".join(sorted(ADAPTERS))
        raise ValueError(f"Unknown company adapter {name!r}; available adapters: {available}") from exc
    return adapter_type()

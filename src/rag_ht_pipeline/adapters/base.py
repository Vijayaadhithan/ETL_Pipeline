from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from ..config import PipelineConfig


class CompanyAdapter(ABC):
    name: str

    @abstractmethod
    def validate_sources(self, config: PipelineConfig) -> dict[str, Any]:
        """Validate the source files and schema without modifying them."""

    @abstractmethod
    def normalize(
        self,
        config: PipelineConfig,
        *,
        sample_size: int | None = None,
        strict_subcategory_consistency: bool = False,
    ) -> dict[str, Any]:
        """Build the canonical pre-retrieval parquet consumed by shared stages."""

    def run_legacy_stage(
        self,
        stage: str,
        config: PipelineConfig,
        *,
        sample_size: int | None = None,
        strict_subcategory_consistency: bool = False,
    ) -> dict[str, Any]:
        raise ValueError(f"Adapter {self.name!r} does not support legacy stage {stage!r}.")

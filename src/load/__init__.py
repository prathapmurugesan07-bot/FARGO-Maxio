"""Load layer for Maxio."""

from .azure_ingest_maxio import main
from .test_maxio import main as test_main

__all__ = ["main", "test_main"]

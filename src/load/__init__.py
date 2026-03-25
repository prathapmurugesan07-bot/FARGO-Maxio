"""Load layer for Maxio."""

from .azure_ingest_maxio import main
from .azure_ingest_maxio_staging import main as staging_main
from .azure_ingest_maxio_both import main as both_main
from .test_maxio import main as test_main

__all__ = ["main", "staging_main", "both_main", "test_main"]

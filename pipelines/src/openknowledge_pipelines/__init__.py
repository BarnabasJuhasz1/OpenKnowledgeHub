"""Prefect flows and the okp command line."""

import os

# Prefect reads this when it is imported, so it must be set before any module imports Prefect.
os.environ.setdefault("PREFECT_SERVER_ANALYTICS_ENABLED", "false")

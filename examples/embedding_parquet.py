"""Demonstrate the installed local converter; local receipts do not prove archive lineage."""

from openknowledge_adapters.parquet.local import convert, main

__all__ = ["convert"]

if __name__ == "__main__":
    main()

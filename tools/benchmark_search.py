"""Offline bounded evaluator; no clients, models, or services are started."""

import argparse
import sys
from pathlib import Path
from time import perf_counter

from pydantic import ValidationError

from openknowledge_core.search.pilot import PilotInput, evaluate

MAX_BYTES = 16 * 1024 * 1024


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="pilot JSON, at most 16 MiB")
    args = parser.parse_args()
    try:
        with args.input.open("rb") as stream:
            payload = stream.read(MAX_BYTES + 1)
        if len(payload) > MAX_BYTES:
            raise ValueError("input exceeds 16 MiB")
        pilot = PilotInput.model_validate_json(payload)
        start = perf_counter()
        result = evaluate(pilot)
        print(result.model_dump_json())
        print(f"exact_evaluation_seconds={perf_counter() - start:.6f}", file=sys.stderr)
    except (OSError, ValueError, ValidationError):
        # Never echo exported vectors, text, or arbitrary paths in validation output.
        print(
            "invalid pilot input; check schema, bounds, provenance and candidate ids",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

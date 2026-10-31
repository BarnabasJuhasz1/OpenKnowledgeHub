"""Network-free indexing CPU comparison: uv run python -m tests.search_index_benchmark."""

import asyncio
import json
import multiprocessing
import resource
import time
from concurrent.futures import ProcessPoolExecutor

from openknowledge_core.search.commands import IndexPapers, IndexPapersResult
from tests.fakes.search_parts import generated_index, index_generated


async def _inline(commands: list[IndexPapers]) -> list[IndexPapersResult]:
    return list(await asyncio.gather(*(generated_index(c) for c in commands)))


def main() -> None:
    commands = [
        IndexPapers(
            index="papers-v2-benchmark", release="2026-10-27", part=part, parts=2, bounds=(1, 40000)
        )
        for part in (1, 2)
    ]
    start = time.perf_counter()
    before = asyncio.run(_inline(commands))
    baseline = time.perf_counter() - start
    with ProcessPoolExecutor(2, mp_context=multiprocessing.get_context("spawn")) as workers:
        start = time.perf_counter()
        after = list(workers.map(index_generated, commands))
        cold = time.perf_counter() - start
        start = time.perf_counter()
        warm = list(workers.map(index_generated, commands))
        reused = time.perf_counter() - start
        assert before == after == warm
    print(
        json.dumps(
            {
                "documents": sum(r.written for r in before),
                "inline_seconds": baseline,
                "spawn_seconds": cold,
                "reused_seconds": reused,
                "parent_peak_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                "max_child_peak_kib": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

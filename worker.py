from operator import itemgetter
from typing import Any

from settings import (
    NUM_THREADS,
    PROCESSING_TIME_PER_GB,
)


def _safedraw(lst: list[int]) -> tuple[int | None, list[int]]:
    if lst:
        return lst[0], lst[1:]
    else:
        return None, lst


def do_worker(worker_load: tuple[str, list[int]]) -> dict[int, Any]:
    """
    input:
        (
            "large",
            [1000, 900, 800],
        )

    output:
        {
            0: {
                "time": 123,
                "sizes": [90, 70, ...],
            },
            1: {
                "time": 123,
                "sizes": [90, 70, ...],
            },
            ...
        }
    """
    tier = worker_load[0]
    size_list = [size for size in worker_load[1]]
    num_threads = NUM_THREADS[tier]

    results = {tid: {"sizes": []} for tid in range(num_threads)}

    clock = 0
    threads = {tid: (clock, None) for tid in range(num_threads)}  # values: (start, size)

    while True:
        # fill empty threads if there are todo
        for tid, tv in threads.items():
            if tv[1] is None:
                top, size_list = _safedraw(size_list)
                threads[tid] = (clock, top)

        # look for next to complete based on smallest 'remaining time'
        # i.e. size*PROCESSING_TIME_PER_GB-(clock-initial_time_for_sstable)
        next_events = [
            (tid, tv[1]*PROCESSING_TIME_PER_GB + tv[0] - clock)
            for tid, tv in threads.items()
            if tv[1] is not None
        ]
        if next_events:
            next_tid, next_clockdelta = min(next_events, key=itemgetter(1))
            # we found the next thing that happens.
            clock += next_clockdelta
            results[next_tid]["sizes"].append(threads[next_tid][1])
            results[next_tid]["time"] = clock
            threads[next_tid] = (clock, None) # 'clock' not really useful... (but elegant)

        if not size_list and all(tv[1] is None for tv in threads.values()):
            break

    return results

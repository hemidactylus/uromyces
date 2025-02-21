import json

from settings import (
    MIGRATION_SMALL_TIER_MAX_SSTABLE_SIZE_GB,
    MIGRATION_MEDIUM_TIER_MAX_SSTABLE_SIZE_GB,
)

from worker import do_worker

worker_loads = [
    (
        "large",
        [1000, 900, 800],
    ),
    (
        "large",
        [1200, 920, 820],
    ),
    (
        "medium",
        [480, 400, 300, 300, 300, 150],
    ),
    (
        "small",
        [90, 80, 80, 80, 80, 80, 80, 10, 2, 2, 2],
    ),
]

def check_input(w_loads: list[tuple[str, list[int]]]) -> None:
    # check tier sizes are ok
    # check larges, then mediums, then smalls
    # check labels in large/medium/small
    # check sorted sizes
    return


if __name__ == "__main__":
    output = [do_worker(wl) for wl in worker_loads]
    print(json.dumps(output, indent=2, sort_keys=True))

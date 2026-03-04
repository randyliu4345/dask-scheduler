#!/usr/bin/env python3
"""
Example: run the spot-scheduler MVP against a small synthetic DAG.

Usage
-----
# 1) Start a local Dask cluster (workers auto-tagged for demo purposes):
python example.py --profile          # profiling run  (records runtimes)
python example.py --deadline 60      # optimised run  (spot/on-demand)

# With a real cluster, tag workers when launching them:
#   dask-worker <scheduler> --resources "spot=1"
#   dask-worker <scheduler> --resources "ondemand=1"
"""

from __future__ import annotations

import argparse
import logging
import time

import dask.distributed as dd

from spot_scheduler.runner import run_dag

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-28s  %(levelname)-8s  %(message)s",
)

# ---------------------------------------------------------------------------
# Synthetic DAG
# ---------------------------------------------------------------------------
#
#       A
#      / \
#     B   C
#      \ /
#       D
#       |
#       E
#
DAG = {
    "A": [],
    "B": ["A"],
    "C": ["A"],
    "D": ["B", "C"],
    "E": ["D"],
}

# Simulated workloads (seconds of sleep)
SIMULATED_DURATIONS = {
    "A": 2.0,
    "B": 4.0,
    "C": 1.5,
    "D": 3.0,
    "E": 2.5,
}


def demo_task(task_name: str, *dep_results: object) -> str:
    """Simulate work by sleeping for the task's duration."""
    duration = SIMULATED_DURATIONS.get(task_name, 1.0)
    time.sleep(duration)
    return f"{task_name}:done"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="Spot-scheduler demo")
    parser.add_argument(
        "--profile",
        action="store_true",
        help="Run in profiling mode (all on-demand, record runtimes).",
    )
    parser.add_argument(
        "--deadline",
        type=float,
        default=30.0,
        help="Deadline in seconds for the optimised run (default: 30).",
    )
    parser.add_argument(
        "--scheduler",
        type=str,
        default=None,
        help="Scheduler address (default: spin up a local cluster).",
    )
    parser.add_argument(
        "--profile-path",
        type=str,
        default="runtimes.json",
        help="Path for the runtime profile JSON.",
    )
    parser.add_argument(
        "--buffer",
        type=float,
        default=0.2,
        help="Spot interrupt buffer factor (default: 0.2 = 20%%).",
    )
    args = parser.parse_args()

    # Connect to a real scheduler or start a local one
    if args.scheduler:
        client = dd.Client(args.scheduler)
    else:
        # Local cluster with fake resource tags so both pools exist
        cluster = dd.LocalCluster(
            n_workers=4,
            threads_per_worker=1,
            resources={"spot": 1, "ondemand": 1},
        )
        client = dd.Client(cluster)

    print(f"Dashboard: {client.dashboard_link}")

    results = run_dag(
        client=client,
        dag=DAG,
        task_fn=demo_task,
        deadline=args.deadline,
        profile_path=args.profile_path,
        spot_interrupt_buffer=args.buffer,
        profiling_mode=args.profile,
    )

    print("\n=== Results ===")
    for task, result in results.items():
        print(f"  {task}: {result}")

    client.close()


if __name__ == "__main__":
    main()

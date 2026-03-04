#!/usr/bin/env python3
"""
Example: run the spot-scheduler MVP against a small synthetic DAG.

Usage
-----
# 1) Start a local Dask cluster (workers auto-tagged for demo purposes):
python example.py --profile          # profiling run  (records runtimes)
python example.py --deadline 60      # optimised run  (spot/on-demand)

# Test dynamic re-scheduling with simulated interruptions:
python example.py --deadline 60 --simulate-interruptions B,C

# With a real cluster, tag workers when launching them:
#   dask-worker <scheduler> --resources "spot=1"
#   dask-worker <scheduler> --resources "ondemand=1"
"""

from __future__ import annotations

import argparse
import logging
import time
from typing import Set

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

# Global state for interruption simulation
_interruption_tasks: Set[str] = set()
_interruption_attempts: dict[str, int] = {}
_completion_factor: float = 1.0


def create_demo_task_with_attempts(attempts_dict: dict[str, int]):
    """Create a demo_task function that uses the shared attempts dictionary."""
    def demo_task(task_name: str, *dep_results: object) -> str:
        """Simulate work by sleeping for the task's duration.
        
        If task_name is in _interruption_tasks, will raise an error on first attempt
        to simulate a spot instance interruption.
        """
        import logging
        task_logger = logging.getLogger("example.demo_task")
        
        # Check if this task should be interrupted
        if task_name in _interruption_tasks:
            # Use the shared attempts dictionary passed from the client
            attempt = attempts_dict.get(task_name, 0)
            task_logger.info("Task %s: attempt=%d, interruption_tasks=%s", 
                            task_name, attempt, _interruption_tasks)
            if attempt == 0:
                # First attempt - simulate interruption
                # Note: We can't modify the dict from worker, but that's OK
                # The client will increment it when resubmitting
                task_logger.warning("Task %s: Simulating interruption (first attempt)", task_name)
                raise RuntimeError(f"Spot instance interrupted for task {task_name}")
            # Subsequent attempts succeed (resubmitted to on-demand)
            # Once attempt > 0, the task should always succeed
            task_logger.info("Task %s: Resubmitted attempt (should succeed)", task_name)
        
        duration = SIMULATED_DURATIONS.get(task_name, 1.0) * _completion_factor
        task_logger.debug("Task %s: Executing for %.2f seconds", task_name, duration)
        time.sleep(duration)
        task_logger.info("Task %s: Completed successfully", task_name)
        return f"{task_name}:done"
    
    return demo_task


def demo_task(task_name: str, *dep_results: object) -> str:
    """Simulate work by sleeping for the task's duration.
    
    If task_name is in _interruption_tasks, will raise an error on first attempt
    to simulate a spot instance interruption.
    """
    import logging
    task_logger = logging.getLogger("example.demo_task")
    
    # Check if this task should be interrupted
    if task_name in _interruption_tasks:
        attempt = _interruption_attempts.get(task_name, 0)
        task_logger.info("Task %s: attempt=%d, interruption_tasks=%s", 
                        task_name, attempt, _interruption_tasks)
        if attempt == 0:
            # First attempt - simulate interruption
            task_logger.warning("Task %s: Simulating interruption (first attempt)", task_name)
            raise RuntimeError(f"Spot instance interrupted for task {task_name}")
        # Subsequent attempts succeed (resubmitted to on-demand)
        # Once attempt > 0, the task should always succeed
        task_logger.info("Task %s: Resubmitted attempt (should succeed)", task_name)
    
    duration = SIMULATED_DURATIONS.get(task_name, 1.0) * _completion_factor
    task_logger.debug("Task %s: Executing for %.2f seconds", task_name, duration)
    time.sleep(duration)
    task_logger.info("Task %s: Completed successfully", task_name)
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
    parser.add_argument(
        "--simulate-interruptions",
        type=str,
        default=None,
        help="Comma-separated list of task names to simulate spot interruptions for (e.g., 'B,C').",
    )
    parser.add_argument(
        "--early-completion",
        type=float,
        default=1.0,
        help="Factor to multiply task durations by (default: 1.0). Use <1.0 for faster completion.",
    )
    args = parser.parse_args()
    
    # Parse interruption simulation tasks
    global _interruption_tasks, _completion_factor, _client
    if args.simulate_interruptions:
        _interruption_tasks = {t.strip() for t in args.simulate_interruptions.split(",")}
        print(f"⚠️  Simulating spot interruptions for tasks: {_interruption_tasks}")
    
    # Set completion factor
    _completion_factor = args.early_completion
    if args.early_completion != 1.0:
        print(f"⏱️  Task completion factor: {args.early_completion}x (tasks will complete {'faster' if args.early_completion < 1.0 else 'slower'})")

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

    # Create a task function that checks distributed state for attempt counter
    # Don't capture client in closure - use get_client() inside the function instead
    def task_fn_with_attempts(task_name: str, *dep_results: object) -> str:
        """Task function that checks distributed state for attempt counter."""
        import logging
        from distributed import get_client
        
        task_logger = logging.getLogger("example.demo_task")
        
        # Check if this task should be interrupted
        if task_name in _interruption_tasks:
            # Try to get attempt count from distributed state first
            attempt = 0
            try:
                # Get client from distributed context (works on workers)
                worker_client = get_client()
                task_logger.debug("Got worker client for task %s", task_name)
                dist_attempt = worker_client.get_dataset(f"_attempt_{task_name}", None)
                task_logger.debug("Retrieved distributed state for %s: %s", task_name, dist_attempt)
                if dist_attempt is not None:
                    # get_dataset returns the value directly, not a dict
                    attempt = dist_attempt
                    task_logger.info("Task %s: Using distributed state attempt=%d", task_name, attempt)
                else:
                    # Fall back to local state
                    attempt = _interruption_attempts.get(task_name, 0)
                    task_logger.info("Task %s: Distributed state not found, using local attempt=%d", task_name, attempt)
            except Exception as e:
                # Fall back to local state if distributed state not available
                task_logger.warning("Could not get distributed state for %s: %s, using local state", task_name, str(e))
                attempt = _interruption_attempts.get(task_name, 0)
            
            task_logger.info("Task %s: final attempt=%d", task_name, attempt)
            if attempt == 0:
                # First attempt - simulate interruption
                task_logger.warning("Task %s: Simulating interruption (first attempt)", task_name)
                raise RuntimeError(f"Spot instance interrupted for task {task_name}")
            # Subsequent attempts succeed (resubmitted to on-demand)
            task_logger.info("Task %s: Resubmitted attempt %d (should succeed)", task_name, attempt)
        
        duration = SIMULATED_DURATIONS.get(task_name, 1.0) * _completion_factor
        task_logger.debug("Task %s: Executing for %.2f seconds", task_name, duration)
        time.sleep(duration)
        task_logger.info("Task %s: Completed successfully", task_name)
        return f"{task_name}:done"

    results = run_dag(
        client=client,
        dag=DAG,
        task_fn=task_fn_with_attempts,
        deadline=args.deadline,
        profile_path=args.profile_path,
        spot_interrupt_buffer=args.buffer,
        profiling_mode=args.profile,
        interruption_attempts=_interruption_attempts,  # Pass the attempts dict
    )

    print("\n=== Results ===")
    for task, result in results.items():
        print(f"  {task}: {result}")

    client.close()


if __name__ == "__main__":
    main()

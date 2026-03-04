"""
High-level runner that ties everything together:

1. Load (or profile) runtimes.
2. Compute critical path & slack.
3. Assign tasks to spot / on-demand.
4. Submit tasks through a Dask client with the right ``resources`` tags.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import dask.distributed as dd

from .assignment import assign_instances
from .dag import critical_path, topo_sort
from .plugin import ProfilingPlugin

logger = logging.getLogger("spot_scheduler.runner")


def run_dag(
    client: dd.Client,
    dag: Dict[str, List[str]],
    task_fn: Callable[..., Any],
    deadline: float,
    profile_path: str | Path = "runtimes.json",
    spot_interrupt_buffer: float = 0.2,
    profiling_mode: bool = False,
) -> Dict[str, Any]:
    """Execute a task DAG on a Dask cluster with spot-aware scheduling.

    Parameters
    ----------
    client : dask.distributed.Client
        Connected Dask client.
    dag : dict[str, list[str]]
        Task DAG — ``{task_name: [dependency_names, ...]}``.
    task_fn : callable
        ``task_fn(task_name, *resolved_dep_results) -> result``.
        Called for each task with the task key and the results of its
        dependency tasks.
    deadline : float
        Overall wall-clock deadline in seconds for the full DAG.
    profile_path : str or Path
        Path for the runtime JSON profile.
    spot_interrupt_buffer : float
        Fraction of task runtime used as safety margin (default 0.2 = 20 %).
    profiling_mode : bool
        If *True*, every task runs on on-demand and runtimes are recorded.
        On *False* (the default), a previous profile is loaded to compute
        the critical path and make spot/on-demand assignments.

    Returns
    -------
    dict[str, Any]
        Mapping of task name → computed result.
    """
    profile_path = Path(profile_path)

    # ------------------------------------------------------------------
    # PROFILING RUN — record runtimes on on-demand workers
    # ------------------------------------------------------------------
    if profiling_mode:
        logger.info("=== PROFILING RUN — all tasks on on-demand ===")
        plugin = ProfilingPlugin(profile_path=profile_path)
        client.register_plugin(plugin)

        futures: Dict[str, dd.Future] = {}
        for task in topo_sort(dag):
            dep_futures = [futures[d] for d in dag[task]]
            futures[task] = client.submit(
                task_fn,
                task,
                *dep_futures,
                resources={"ondemand": 1},
                key=task,
            )

        dd.wait(list(futures.values()))
        # Force save (the plugin also saves on scheduler close)
        client.run_on_scheduler(lambda dask_scheduler: [
            p.save()
            for p in dask_scheduler.plugins.values()
            if hasattr(p, "save")
        ])
        logger.info("Profiling complete — runtimes saved to %s", profile_path)
        return {k: f.result() for k, f in futures.items()}

    # ------------------------------------------------------------------
    # OPTIMISED RUN — use profiled runtimes for spot/on-demand assignment
    # ------------------------------------------------------------------
    runtimes = ProfilingPlugin.load(profile_path)
    logger.info("Loaded runtimes for %d tasks from %s", len(runtimes), profile_path)

    assignment = assign_instances(
        dag, runtimes, deadline, spot_interrupt_buffer
    )

    cp = critical_path(dag, runtimes)
    spot_count = sum(1 for v in assignment.values() if v == "spot")
    od_count = len(assignment) - spot_count

    logger.info("Critical path: %s", " → ".join(cp))
    logger.info("Assignment: %d spot, %d on-demand", spot_count, od_count)
    for task, inst in assignment.items():
        logger.info("  %-20s → %s (slack info)", task, inst)

    futures = {}
    for task in topo_sort(dag):
        dep_futures = [futures[d] for d in dag[task]]
        resource = (
            {"spot": 1} if assignment[task] == "spot" else {"ondemand": 1}
        )
        futures[task] = client.submit(
            task_fn,
            task,
            *dep_futures,
            resources=resource,
            key=task,
        )

    dd.wait(list(futures.values()))
    return {k: f.result() for k, f in futures.items()}

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
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import dask.distributed as dd
from distributed.client import FuturesCancelledError

from .assignment import assign_instances, reassign_instances
from .dag import critical_path, topo_sort
from .plugin import ProfilingPlugin
from .rescheduling_plugin import DynamicReschedulingPlugin

logger = logging.getLogger("spot_scheduler.runner")


def run_dag(
    client: dd.Client,
    dag: Dict[str, List[str]],
    task_fn: Callable[..., Any],
    deadline: float,
    profile_path: str | Path = "runtimes.json",
    spot_interrupt_buffer: float = 0.2,
    profiling_mode: bool = False,
    interruption_attempts: Dict[str, int] | None = None,
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

    # Register rescheduling plugin for dynamic re-scheduling
    rescheduling_plugin = DynamicReschedulingPlugin(
        initial_assignment=assignment,
        initial_runtimes=runtimes,
        deadline=deadline,
        spot_interrupt_buffer=spot_interrupt_buffer,
    )
    client.register_plugin(rescheduling_plugin)

    futures: Dict[str, dd.Future] = {}
    submitted_tasks: set[str] = set()
    resubmitted_tasks: set[str] = set()  # Track tasks that have been resubmitted to on-demand
    start_time = time.time()

    # Submit initial batch of ready tasks
    def submit_ready_tasks():
        """Submit all tasks that are ready (dependencies satisfied)."""
        for task in topo_sort(dag):
            if task in submitted_tasks:
                continue
            deps = dag[task]
            # Check if all dependencies are completed successfully
            deps_ready = True
            dep_futures = []
            for d in deps:
                if d not in futures:
                    deps_ready = False
                    break
                dep_future = futures[d]
                if not dep_future.done():
                    deps_ready = False
                    break
                # Check if dependency completed successfully
                try:
                    dep_future.result()  # This will raise if task failed
                    dep_futures.append(dep_future)
                except Exception:
                    # Dependency failed - it will be resubmitted, wait for it
                    deps_ready = False
                    break

            if deps_ready:
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
                submitted_tasks.add(task)

    # Initial submission
    submit_ready_tasks()

    # Monitor and re-schedule as tasks complete
    while len(submitted_tasks) < len(dag) or any(
        not f.done() for f in futures.values()
    ):
        if not futures:
            submit_ready_tasks()
            if not futures:
                time.sleep(0.1)
                continue
        
        # Check for failed futures first
        failed_tasks = []
        active_futures = []
        for task, future in list(futures.items()):
            if future.status == "cancelled":
                if task in submitted_tasks:
                    submitted_tasks.remove(task)
                if task in futures:
                    del futures[task]
                continue
            if future.done():
                try:
                    future.result()
                except Exception as e:
                    error_msg = str(e).lower()
                    
                    # 1. Handle intentional cancellations
                    if "cancellederror" in str(type(e)).lower() or "cancelled" in error_msg:
                        if task in submitted_tasks:
                            submitted_tasks.remove(task)
                        if task in futures:
                            del futures[task]
                        continue
                        
                    # 2. Handle expected Spot interruptions (by assignment OR by exception text)
                    is_spot_interruption = (
                        "spot instance interrupted" in error_msg or 
                        (task in assignment and assignment[task] == "spot")
                    )
                    
                    if is_spot_interruption and task not in resubmitted_tasks:
                        failed_tasks.append(task)
                        continue  # Let the outer loop's interrupted block handle the rescheduling
                        
                    # 3. Handle a task that failed AGAIN after being moved to on-demand
                    if task in resubmitted_tasks:
                        logger.error("Resubmitted task %s failed again! Exception: %s", task, e)
                        raise RuntimeError(f"Task {task} failed on retry (on-demand): {e}") from e
                        
                    # 4. Handle all other unexpected failures (e.g., division by zero, missing file)
                    logger.error("Task %s failed with a fatal error! Exception: %s", task, e)
                    raise RuntimeError(f"Task {task} failed with exception: {e}") from e
            else:
                active_futures.append(future)
        
        # Wait for futures to complete
        not_done = None
        if active_futures:
            try:
                done, not_done = dd.wait(active_futures, return_when="FIRST_COMPLETED", timeout=0.5)
            except TimeoutError:
                submit_ready_tasks()
            except FuturesCancelledError:
                submit_ready_tasks()
        else:
            submit_ready_tasks()
            if not failed_tasks:
                if not futures:
                    time.sleep(0.1)

        # Check plugin state AFTER waiting (catches interruptions during the wait)
        plugin_state = client.run_on_scheduler(
            lambda dask_scheduler: next(
                (
                    {
                        "completed": p.get_completed_tasks(),
                        "interrupted": p.get_interrupted_tasks(),
                        "completion_times": p.get_completion_times(),
                        "elapsed": p.get_elapsed_time(),
                    }
                    for p in dask_scheduler.plugins.values()
                    if isinstance(p, DynamicReschedulingPlugin)
                ),
                None,
            )
        )

        # Initialize state
        interrupted = set()
        completed = set()
        completion_times = {}
        elapsed = time.time() - start_time

        if plugin_state:
            interrupted = set(plugin_state["interrupted"])  # Convert to mutable set
            completed = plugin_state["completed"]
            completion_times = plugin_state["completion_times"]
            elapsed = plugin_state["elapsed"]
        else:
            logger.warning("No plugin state found!")
        
        # Merge failed_tasks into interrupted set
        if failed_tasks:
            for task in failed_tasks:
                if task not in interrupted and task not in resubmitted_tasks:
                    interrupted.add(task)
                    logger.info("Detected failed spot task %s", task)

        # Process all interrupted tasks
        handled_interrupted = set()
        if interrupted or len(completed) > 0:
            #if interrupted:
            #    logger.info("Re-scheduling: %d interrupted, %d completed", len(interrupted), len(completed))

            # Recompute assignment for remaining tasks
            new_assignment = reassign_instances(
                dag=dag,
                runtimes=runtimes,
                deadline=deadline,
                completion_times=completion_times,
                elapsed_time=elapsed,
                spot_interrupt_buffer=spot_interrupt_buffer,
                interrupted_tasks=interrupted,
            )

            for task in interrupted:
                # 1. ALWAYS mark it as handled so the plugin clears it from its state
                handled_interrupted.add(task)

                # 2. Skip if we've already dealt with it
                if task in resubmitted_tasks:
                    continue
                    
                if task in futures:
                    logger.info("Resubmitting interrupted task %s to on-demand", task)
                    
                    if interruption_attempts is not None:
                        interruption_attempts[task] = interruption_attempts.get(task, 0) + 1
                        new_attempt = interruption_attempts[task]
                        try:
                            client.publish_dataset({f"_attempt_{task}": new_attempt})
                        except Exception:
                            pass
                    
                    try:
                        client.cancel(futures[task], force=True)
                    except Exception:
                        pass
                    if task in futures:
                        del futures[task]
                    if task in submitted_tasks:
                        submitted_tasks.remove(task)
                    assignment[task] = "on-demand"
                    resubmitted_tasks.add(task)
                    # Note: Task will be resubmitted by submit_ready_tasks() if dependencies are ready
                    
                elif task not in submitted_tasks:
                    # Task was interrupted but not yet submitted - update assignment and try to submit
                    assignment[task] = "on-demand"
                    # Don't mark as resubmitted yet - only mark when actually submitted
                    # Check if dependencies are ready and submit immediately if so
                    deps = dag[task]
                    deps_ready = True
                    dep_futures = []
                    for d in deps:
                        if d not in futures:
                            deps_ready = False
                            break
                        dep_future = futures[d]
                        if not dep_future.done():
                            deps_ready = False
                            break
                        try:
                            dep_future.result()  # Check if dependency succeeded
                            dep_futures.append(dep_future)
                        except Exception:
                            deps_ready = False
                            break
                    
                    if deps_ready:
                        logger.info("Resubmitting interrupted task %s to on-demand (was not yet submitted)", task)
                        if interruption_attempts is not None:
                            interruption_attempts[task] = interruption_attempts.get(task, 0) + 1
                            new_attempt = interruption_attempts[task]
                            try:
                                client.publish_dataset({f"_attempt_{task}": new_attempt})
                            except Exception:
                                pass
                        futures[task] = client.submit(
                            task_fn,
                            task,
                            *dep_futures,
                            resources={"ondemand": 1},
                            key=task,
                        )
                        submitted_tasks.add(task)
                        resubmitted_tasks.add(task)  # Only mark as resubmitted when actually submitted
                    # If deps not ready, submit_ready_tasks() will handle it later
            
            # Clear handled interrupted tasks from plugin (only after all are processed)
            if handled_interrupted:
                client.run_on_scheduler(
                    lambda dask_scheduler, tasks=handled_interrupted: [
                        p.interrupted_tasks.discard(t) 
                        for p in dask_scheduler.plugins.values()
                        if isinstance(p, DynamicReschedulingPlugin)
                        for t in tasks
                    ]
                )
            
            submit_ready_tasks()

            for task in new_assignment:
                if task not in submitted_tasks:
                    assignment[task] = new_assignment[task]

        submit_ready_tasks()
        
        # If we handled interruptions, check again immediately for any new ones
        # This ensures we catch multiple interruptions that happen simultaneously
        if handled_interrupted:
            continue
        
        if not not_done and len(submitted_tasks) < len(dag):
            submit_ready_tasks()
            if not futures:
                time.sleep(0.1)

    if futures:
        dd.wait(list(futures.values()))
        return {k: f.result() for k, f in futures.items()}
    else:
        return {}

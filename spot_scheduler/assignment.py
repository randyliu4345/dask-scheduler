"""
Spot / on-demand assignment heuristic.

The rule is simple:
  - If a task has enough slack to absorb a spot interruption
    (slack > buffer_factor × task_runtime), assign it to **spot**.
  - Otherwise assign it to **on-demand**.
"""

from __future__ import annotations

from typing import Dict, Literal, Set

from .dag import compute_slack, compute_slack_with_actuals

InstanceType = Literal["spot", "on-demand"]


def assign_instances(
    dag: Dict[str, list],
    runtimes: Dict[str, float],
    deadline: float,
    spot_interrupt_buffer: float = 0.2,
) -> Dict[str, InstanceType]:
    """Decide whether each task should run on a spot or on-demand worker.

    Parameters
    ----------
    dag : dict
        Task DAG (task → list of dependency task names).
    runtimes : dict
        Profiled runtimes in seconds for each task.
    deadline : float
        The overall deadline for the DAG in seconds.
    spot_interrupt_buffer : float
        Safety factor — a task is eligible for spot only when
        ``slack > spot_interrupt_buffer * runtime``.

    Returns
    -------
    dict[str, "spot" | "on-demand"]
        Mapping of task name → instance type.
    """
    slack = compute_slack(dag, runtimes, deadline)

    assignment: Dict[str, InstanceType] = {}
    for task, s in slack.items():
        if s > spot_interrupt_buffer * runtimes[task]:
            assignment[task] = "spot"
        else:
            assignment[task] = "on-demand"

    return assignment


def reassign_instances(
    dag: Dict[str, list],
    runtimes: Dict[str, float],
    deadline: float,
    completion_times: Dict[str, float],
    elapsed_time: float,
    spot_interrupt_buffer: float = 0.2,
    interrupted_tasks: Set[str] | None = None,
    current_assignment: Dict[str, InstanceType] | None = None,
) -> Dict[str, InstanceType]:
    """Recompute assignment for remaining tasks based on actual completion times.

    This function is called during dynamic re-scheduling when tasks complete
    early/late or spot instances are interrupted.

    Parameters
    ----------
    dag : dict
        Task DAG (task → list of dependency task names).
    runtimes : dict
        Profiled runtimes in seconds for each task.
    deadline : float
        The overall deadline for the DAG in seconds.
    completion_times : dict
        Actual completion times (wall-clock) for completed tasks.
    elapsed_time : float
        Current elapsed wall-clock time since start.
    spot_interrupt_buffer : float
        Safety factor — a task is eligible for spot only when
        ``slack > spot_interrupt_buffer * runtime``.
    interrupted_tasks : set[str], optional
        Set of tasks that were interrupted (will be reassigned to on-demand).
    current_assignment : dict[str, InstanceType], optional
        Current assignment (used to preserve assignments for running tasks).

    Returns
    -------
    dict[str, "spot" | "on-demand"]
        Mapping of task name → instance type for remaining tasks.
    """
    if interrupted_tasks is None:
        interrupted_tasks = set()

    completed = set(completion_times.keys())
    remaining = set(dag.keys()) - completed

    if not remaining:
        # All tasks completed
        return {}

    # Compute slack based on actual completion times
    slack = compute_slack_with_actuals(
        dag, runtimes, deadline, completion_times, elapsed_time
    )

    assignment: Dict[str, InstanceType] = {}
    for task in remaining:
        # Interrupted tasks always go to on-demand
        if task in interrupted_tasks:
            assignment[task] = "on-demand"
        elif slack[task] > spot_interrupt_buffer * runtimes[task]:
            assignment[task] = "spot"
        else:
            assignment[task] = "on-demand"

    return assignment

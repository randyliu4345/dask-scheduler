"""
Spot / on-demand assignment heuristic.

The rule is simple:
  - If a task has enough slack to absorb a spot interruption
    (slack > buffer_factor × task_runtime), assign it to **spot**.
  - Otherwise assign it to **on-demand**.
"""

from __future__ import annotations

from typing import Dict, Literal

from .dag import compute_slack

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

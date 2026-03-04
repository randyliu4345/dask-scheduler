"""
DAG utilities: topological sort, critical-path analysis, and slack computation.
"""

from __future__ import annotations

from collections import deque
from typing import Dict, List


# ---------------------------------------------------------------------------
# Topological sort (Kahn's algorithm)
# ---------------------------------------------------------------------------


def topo_sort(dag: Dict[str, List[str]]) -> List[str]:
    """Return a topological ordering of *dag*.

    Parameters
    ----------
    dag : dict[str, list[str]]
        Mapping of ``task -> [dependencies]``.

    Returns
    -------
    list[str]
        Tasks in valid execution order (dependencies before dependents).

    Raises
    ------
    ValueError
        If the graph contains a cycle.
    """
    # Build adjacency list and in-degree map
    in_degree: Dict[str, int] = {t: 0 for t in dag}
    children: Dict[str, List[str]] = {t: [] for t in dag}

    for task, deps in dag.items():
        for dep in deps:
            children[dep].append(task)
            in_degree[task] += 1

    queue: deque[str] = deque(t for t, d in in_degree.items() if d == 0)
    order: List[str] = []

    while queue:
        node = queue.popleft()
        order.append(node)
        for child in children[node]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    if len(order) != len(dag):
        raise ValueError("DAG contains a cycle – topological sort impossible")

    return order


# ---------------------------------------------------------------------------
# Critical-path / slack analysis
# ---------------------------------------------------------------------------


def _build_successors(dag: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """Return a mapping of task → list of immediate successors."""
    successors: Dict[str, List[str]] = {t: [] for t in dag}
    for task, deps in dag.items():
        for dep in deps:
            successors[dep].append(task)
    return successors


def earliest_finish_times(
    dag: Dict[str, List[str]],
    runtimes: Dict[str, float],
) -> Dict[str, float]:
    """Compute the *earliest finish time* (EFT) for every task.

    EFT(t) = max(EFT(dep) for dep in deps(t)) + runtime(t)
    For root tasks (no deps): EFT(t) = runtime(t).
    """
    eft: Dict[str, float] = {}
    for task in topo_sort(dag):
        deps = dag[task]
        start = max((eft[d] for d in deps), default=0.0)
        eft[task] = start + runtimes[task]
    return eft


def latest_start_times(
    dag: Dict[str, List[str]],
    runtimes: Dict[str, float],
    deadline: float,
) -> Dict[str, float]:
    """Compute the *latest start time* (LST) for every task.

    LST(t) = min(LST(s) for s in successors(t)) - runtime(t)
    For leaf tasks (no successors): LST(t) = deadline - runtime(t).
    """
    successors = _build_successors(dag)
    lst: Dict[str, float] = {}

    for task in reversed(topo_sort(dag)):
        succs = successors[task]
        if not succs:
            lst[task] = deadline - runtimes[task]
        else:
            lst[task] = min(lst[s] for s in succs) - runtimes[task]
    return lst


def compute_slack(
    dag: Dict[str, List[str]],
    runtimes: Dict[str, float],
    deadline: float,
) -> Dict[str, float]:
    """Return the *total slack* for every task.

    slack(t) = LST(t) - EST(t)

    where EST(t) = EFT(t) - runtime(t).

    Tasks with zero (or negative) slack are on the critical path.
    """
    eft = earliest_finish_times(dag, runtimes)
    lst = latest_start_times(dag, runtimes, deadline)

    slack: Dict[str, float] = {}
    for task in dag:
        est = eft[task] - runtimes[task]
        slack[task] = lst[task] - est
    return slack


def critical_path(
    dag: Dict[str, List[str]],
    runtimes: Dict[str, float],
) -> List[str]:
    """Return the list of tasks on the critical path (longest path)."""
    eft = earliest_finish_times(dag, runtimes)
    makespan = max(eft.values())
    deadline = makespan  # critical path = zero slack at makespan

    lst = latest_start_times(dag, runtimes, deadline)
    cp: List[str] = []
    for task in topo_sort(dag):
        est = eft[task] - runtimes[task]
        if abs(lst[task] - est) < 1e-9:
            cp.append(task)
    return cp


# ---------------------------------------------------------------------------
# Dynamic re-scheduling support
# ---------------------------------------------------------------------------


def compute_slack_with_actuals(
    dag: Dict[str, List[str]],
    runtimes: Dict[str, float],
    deadline: float,
    completion_times: Dict[str, float],
    elapsed_time: float,
) -> Dict[str, float]:
    """Compute slack for remaining tasks given actual completion times.

    For completed tasks, use their actual completion times.
    For remaining tasks, compute slack based on current time and remaining deadline.

    Parameters
    ----------
    dag : dict[str, list[str]]
        Task DAG.
    runtimes : dict[str, float]
        Profiled runtimes for all tasks.
    deadline : float
        Original deadline.
    completion_times : dict[str, float]
        Actual completion times (wall-clock) for completed tasks.
    elapsed_time : float
        Current elapsed wall-clock time.

    Returns
    -------
    dict[str, float]
        Slack for each task. Completed tasks have slack = 0.
    """
    completed = set(completion_times.keys())
    remaining = set(dag.keys()) - completed

    if not remaining:
        # All tasks completed
        return {task: 0.0 for task in dag}

    # Normalize completion times to relative times (starting from 0)
    # Use elapsed_time as the reference point (when execution started)
    if completion_times:
        # Convert absolute completion times to relative times
        relative_completion = {
            task: time - elapsed_time for task, time in completion_times.items()
        }
        # Ensure non-negative (tasks can't complete before start)
        relative_completion = {
            task: max(0.0, rel_time) for task, rel_time in relative_completion.items()
        }
    else:
        relative_completion = {}

    # Compute earliest finish times using actual completion times
    eft: Dict[str, float] = {}
    for task in topo_sort(dag):
        if task in completed:
            # Use actual completion time (relative to start)
            eft[task] = relative_completion[task]
        else:
            deps = dag[task]
            if deps:
                # Start after latest dependency finishes
                start = max(
                    (eft.get(d, 0.0) for d in deps),
                    default=0.0,
                )
            else:
                # Root task - can start now (at elapsed_time, which is 0 relative)
                start = 0.0
            eft[task] = start + runtimes[task]

    # Compute latest start times for remaining tasks
    # Adjust deadline based on elapsed time
    remaining_deadline = deadline - elapsed_time
    if remaining_deadline <= 0:
        # Deadline already passed - all remaining tasks have negative slack
        slack: Dict[str, float] = {}
        for task in dag:
            if task in completed:
                slack[task] = 0.0
            else:
                est = eft[task] - runtimes[task]
                slack[task] = remaining_deadline - est
        return slack

    successors = _build_successors(dag)
    lst: Dict[str, float] = {}

    # Process in reverse topological order
    for task in reversed(topo_sort(dag)):
        if task in completed:
            # Already finished - use its actual start time
            lst[task] = eft[task] - runtimes[task]
        else:
            succs = successors[task]
            remaining_succs = [s for s in succs if s not in completed]
            if not remaining_succs:
                # All successors completed or no successors
                lst[task] = remaining_deadline - runtimes[task]
            else:
                lst[task] = min(
                    (lst.get(s, remaining_deadline) for s in remaining_succs),
                    default=remaining_deadline,
                ) - runtimes[task]

    # Compute slack
    slack = {}
    for task in dag:
        if task in completed:
            slack[task] = 0.0
        else:
            est = eft[task] - runtimes[task]
            slack[task] = lst[task] - est

    return slack

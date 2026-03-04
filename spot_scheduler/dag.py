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

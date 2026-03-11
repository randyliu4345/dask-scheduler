"""
Spot / on-demand assignment heuristic.

The rule is simple:
  - If a task has enough slack to absorb a spot interruption
    (slack > buffer_factor × task_runtime), assign it to **spot**.
  - Otherwise assign it to **on-demand**.
"""

from __future__ import annotations

from typing import Dict, Literal, Set, List

from .dag import compute_slack, compute_slack_with_actuals, critical_path, topo_sort

InstanceType = Literal["spot", "ondemand"]


def critical_path_duration(dag: Dict[str, List[str]], runtimes: Dict[str, float]):
    cp = critical_path(dag, runtimes)
    return sum(runtimes[node] for node in cp)


def assign_greedy_cp_increase(
    dag: Dict[str, list],
    runtimes: Dict[str, float],
    deadline: float,
    spot_interrupt_buffer: float,
) -> Dict[str, InstanceType]:
    cp = critical_path_duration(dag, runtimes)
    spot_duration_factor = 2  # in the worse case where the instance is pre-empted right before finishing, total runtime could be doubled
    assignment: Dict[str, InstanceType] = {}
    while cp < deadline and len(assignment) < len(dag):

        def compute_new_cp(node):
            runtimes[node] *= spot_duration_factor
            new_cp = critical_path_duration(dag, runtimes)
            runtimes[node] /= spot_duration_factor
            return new_cp

        next_cp, best_node = min(
            (compute_new_cp(node), node) for node in dag if node not in assignment
        )

        if next_cp < deadline:
            runtimes[best_node] *= spot_duration_factor
            assignment[best_node] = "spot"
            cp = next_cp
        else:
            break

    for node in dag:
        if node not in assignment:
            assignment[node] = "ondemand"

    return assignment

def assign_greedy_cp_increase_fast(
    dag: Dict[str, list],
    runtimes: Dict[str, float],
    deadline: float,
    spot_interrupt_buffer: float,
) -> Dict[str, InstanceType]:
    runtimes = dict(runtimes)
    spot_duration_factor = 1 + spot_interrupt_buffer
    assignment: Dict[str, InstanceType] = {t: "ondemand" for t in dag}

    # build children map once — O(V + E)
    children: Dict[str, list] = {t: [] for t in dag}
    for task, parents in dag.items():
        for p in parents:
            children[p].append(task)

    def compute_distances():
        # dist_from_source[t] = longest path from any source to start of t
        dist_from_source: Dict[str, float] = {}
        for task in topo_sort(dag):
            dist_from_source[task] = max(
                (dist_from_source[p] + runtimes[p] for p in dag[task]), default=0.0
            )

        # dist_from_sink[t] = longest path from end of t to any sink
        dist_to_sink: Dict[str, float] = {}
        for task in reversed(topo_sort(dag)):
            dist_to_sink[task] = max(
                (runtimes[s] + dist_to_sink[s] for s in children[task]), default=0.0
            )

        cp = max(dist_from_source[t] + runtimes[t] + dist_to_sink[t] for t in dag)
        return dist_from_source, dist_to_sink, cp

    dist_from_source, dist_to_sink, cp = compute_distances()

    while cp <= deadline:
        remaining = [t for t in dag if assignment[t] == "ondemand"]
        if not remaining:
            break

        # marginal CP increase if we inflate this task's runtime
        # = current path through task with inflated runtime
        def new_cp_if_spot(task):
            inflation = runtimes[task] * (spot_duration_factor - 1)
            return dist_from_source[task] + runtimes[task] + inflation + dist_to_sink[task]

        best_node = min(remaining, key=new_cp_if_spot)

        if new_cp_if_spot(best_node) <= deadline:
            assignment[best_node] = "spot"
            runtimes[best_node] *= spot_duration_factor
            # recompute after each assignment since distances change
            dist_from_source, dist_to_sink, cp = compute_distances()
        else:
            break

    return assignment


def optimal_assignment_pruned(
    dag: Dict[str, List[str]],
    runtimes: Dict[str, float],
    deadline: float,
    spot_interrupt_buffer: float = 1.0,
) -> Dict[str, InstanceType]:
    tasks        = list(dag.keys())
    factor       = 1 + spot_interrupt_buffer
    effective    = dict(runtimes)  # mutated in place during search
    best: Dict   = {}
    best_score   = [-1.0]          # list so closure can mutate it

    # order tasks by descending runtime — spot high-value tasks first
    # so we find good solutions early and prune more aggressively
    tasks_ordered = sorted(tasks, key=lambda t: runtimes[t], reverse=True)

    def branch(idx: int, current_score: float) -> None:
        if critical_path_duration(dag, effective) > deadline:
            return  # prune — already infeasible regardless of remaining tasks

        if idx == len(tasks_ordered):
            if current_score > best_score[0]:
                best_score[0] = current_score
                best.clear()
                best.update({t: ("spot" if effective[t] != runtimes[t] else "ondemand") for t in tasks})
            return

        task = tasks_ordered[idx]
        remaining_value = sum(runtimes[t] for t in tasks_ordered[idx:])
        if current_score + remaining_value <= best_score[0]:
            return  # prune — can't beat best even if all remaining go to spot

        # try spot first
        effective[task] *= factor
        branch(idx + 1, current_score + runtimes[task])
        effective[task] /= factor

        # try on-demand
        branch(idx + 1, current_score)

    branch(0, 0.0)

    if not best:
        best.update({t: "ondemand" for t in tasks})
    return best


def assign_greedy_runtime(
    dag: Dict[str, list],
    runtimes: Dict[str, float],
    deadline: float,
    spot_interrupt_buffer: float = 1.0,
) -> Dict[str, InstanceType]:
    """
    Same conservative budget approach as assign_greedy_topo but assigns
    spot to the cheapest tasks first, maximising the number (and total
    duration) of spot assignments within the budget.
    """
    assignment: Dict[str, InstanceType] = {t: "ondemand" for t in dag}
    cp = critical_path_duration(dag, runtimes)
    remaining_budget = deadline - cp

    for task in sorted(dag, key=lambda t: runtimes[t]):
        inflation = runtimes[task] * spot_interrupt_buffer
        if remaining_budget >= inflation:
            assignment[task] = "spot"
            remaining_budget -= inflation

    return assignment


def assign_instances(
    dag: Dict[str, list],
    runtimes: Dict[str, float],
    deadline: float,
    spot_interrupt_buffer: float = 1,
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
    return assign_greedy_cp_increase_fast(dag, runtimes, deadline, spot_interrupt_buffer)

def max_parallelism(dag: dict[str, list[str]]) -> int:
    """Maximum number of tasks that can run concurrently at any point in the DAG."""
    levels: dict[str, int] = {}

    for task in topo_sort(dag):
        parents = dag[task]
        levels[task] = max((levels[p] for p in parents), default=-1) + 1

    from collections import Counter
    return max(Counter(levels.values()).values())

import heapq
from collections import defaultdict
from typing import Dict, List

def simulate_run(
    dag: Dict[str, List[str]],
    runtimes: Dict[str, float],
    assignments: Dict[str, InstanceType],
    cluster_instances: Dict[InstanceType, int],
    deadline: float,
    spot_interrupt_buffer: float = 1.0,
) -> float:
    children: Dict[str, List[str]] = defaultdict(list)
    for task, parents in dag.items():
        for p in parents:
            children[p].append(task)

    def effective_runtime(task: str) -> float:
        rt = runtimes[task]
        if assignments[task] == "spot":
            rt *= (1 + spot_interrupt_buffer)
        return rt

    longest_to_exit: Dict[str, float] = {}
    for task in reversed(topo_sort(dag)):
        succs = children[task]
        if not succs:
            longest_to_exit[task] = effective_runtime(task)
        else:
            longest_to_exit[task] = effective_runtime(task) + max(longest_to_exit[s] for s in succs)

    def lst(task: str) -> float:
        return deadline - longest_to_exit[task]

    pending_parents: Dict[str, int] = {task: len(parents) for task, parents in dag.items()}
    ready: set[str] = {t for t, p in pending_parents.items() if p == 0}
    completed: set[str] = set()
    free_workers: Dict[InstanceType, int] = dict(cluster_instances)
    events: List[tuple[float, str]] = []
    now = 0.0

    def dispatch(pool: InstanceType) -> bool:
        for preferred in [pool, "spot" if pool == "ondemand" else "ondemand"]:
            candidates = [t for t in ready if assignments[t] == preferred]
            if not candidates:
                continue
            task = min(candidates, key=lst)
            ready.remove(task)
            free_workers[pool] -= 1
            assignments[task] = pool
            heapq.heappush(events, (now + effective_runtime(task), task))
            return True
        return False

    for pool in ("ondemand", "spot"):
        while free_workers.get(pool, 0) > 0 and ready:
            if not dispatch(pool):
                break

    while events:
        now, task = heapq.heappop(events)
        completed.add(task)
        pool = assignments[task]
        free_workers[pool] += 1

        for child in children[task]:
            pending_parents[child] -= 1
            if pending_parents[child] == 0:
                ready.add(child)

        dispatch(pool)

    return now

def assign_ondemand(dag: Dict[str, list], *args) -> Dict[str, InstanceType]:
    return {task: "ondemand" for task in dag}


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
            assignment[task] = "ondemand"
        elif slack[task] > spot_interrupt_buffer * runtimes[task]:
            assignment[task] = "spot"
        else:
            assignment[task] = "ondemand"

    return assignment

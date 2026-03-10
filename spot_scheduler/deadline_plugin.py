from __future__ import annotations

import logging
import time
from typing import Any, Optional

from distributed import Scheduler
from distributed.diagnostics.plugin import SchedulerPlugin

from spot_scheduler.assignment import InstanceType, assign_instances

logger = logging.getLogger("spot_scheduler")


class DeadlineSchedulerPlugin(SchedulerPlugin):

    name = "deadline-scheduler"

    def __init__(
        self,
        deadline: float,
        spot_interrupt_buffer: float = 0.2,
        default_runtime: float = 1.0,
    ) -> None:
        self.deadline = deadline
        self.spot_interrupt_buffer = spot_interrupt_buffer
        self.default_runtime = default_runtime

        self.start_wall_time: float | None = None
        self.assignment: dict[str, InstanceType] = {}
        self.scheduler: Optional[Scheduler] = None

    def start(self, scheduler: Scheduler) -> None:
        self.scheduler = scheduler
        self.start_wall_time = time.time()

    # ──────────────────────────────────────────────
    # Graph arrival
    # ──────────────────────────────────────────────

    def update_graph(
        self,
        scheduler: Any,
        *,
        client: str,
        keys: set,
        tasks: list,
        annotations: dict[str, dict],
        priority: dict,
        stimulus_id: str,
        **kwargs: Any,
    ) -> None:
        elapsed = self._elapsed()
        remaining_deadline = self.deadline - elapsed

        # build dag + runtimes from scheduler task state
        dag = self._extract_dag(scheduler, tasks)
        runtimes = self._extract_runtimes(scheduler, tasks, annotations)

        # slack = self._compute_slack(dag, runtimes, remaining_deadline)

        spot_ws = self._workers_with(scheduler, "spot")
        od_ws = self._workers_with(scheduler, "ondemand")

        self.assignment = assign_instances(
            dag, runtimes, remaining_deadline, 1.0
        )

        for key in tasks:
            # task_slack = slack.get(key, float("inf"))
            # rt = runtimes.get(key, self.default_runtime)
            # on_spot = task_slack > self.spot_interrupt_buffer * rt

            # self.assignment[key] = "spot" if on_spot else "ondemand"

            # priority[key] = (task_slack,)
            # scheduler.set_restrictions({key: spot_ws if on_spot else od_ws})

            logger.debug("%s → %s", key, self.assignment[key])

    # ──────────────────────────────────────────────
    # Task transitions — promote failed spot tasks
    # ──────────────────────────────────────────────

    def transition(
        self,
        key: Any,
        start: str,
        finish: str,
        *args: Any,
        stimulus_id: str,
        **kwargs: Any,
    ) -> None:
        if not self.scheduler:
            return
        logger.debug("TRANSITION: %s, %s, %s, %s", key, start, finish, stimulus_id)
        # preemption signature: task was in memory, worker died, now needs recompute
        if not (start == "memory" and finish == "released"):
            return
        if "handle-worker-cleanup" not in stimulus_id:
            return
        if self.assignment.get(key) != "spot":
            return

        od_ws = self._workers_with(self.scheduler, "ondemand")
        if not od_ws:
            logger.warning("No on-demand workers available to promote %s", key)
            return

        logger.info("Promoting %s: spot → on-demand", key)
        self.assignment[key] = "ondemand"
        self.scheduler.set_restrictions({key: od_ws})
        # self.scheduler.pr.priorities[key]          = (-1.0,)

    def remove_worker(
        self, scheduler: Scheduler, worker: str, *, stimulus_id: str, **kwargs: Any
    ) -> None:
        spot_ws = self._workers_with(scheduler, "spot")
        # if worker in spot_ws:
        if worker in spot_ws:
            spot_ws.remove(worker)
        for task, instance_type in self.assignment.items():
            if task not in scheduler.tasks:
                continue
            if instance_type == 'spot':
                scheduler.set_restrictions({task: spot_ws})

    def add_worker(self, scheduler: Scheduler, worker: str) -> None:
        spot_ws = self._workers_with(scheduler, "spot")
        # if worker in spot_ws:
        if worker not in spot_ws:
            spot_ws.add(worker)
        for task, instance_type in self.assignment.items():
            if task not in scheduler.tasks:
                continue
            if instance_type == 'spot':
                scheduler.set_restrictions({task: spot_ws})

    # ──────────────────────────────────────────────
    # Extract DAG structure from scheduler task state
    # ──────────────────────────────────────────────

    def _extract_dag(self, scheduler: Any, tasks: list) -> dict[str, list[str]]:
        """Read dependencies directly from scheduler.tasks."""
        dag = {}
        for key in tasks:
            ts = scheduler.tasks.get(key)
            if ts is None:
                dag[key] = []
            else:
                dag[key] = [dep.key for dep in ts.dependencies]
        return dag

    def _extract_runtimes(
        self,
        scheduler: Any,
        tasks: list,
        annotations: dict[str, dict],
    ) -> dict[str, float]:
        """
        Runtime estimates in priority order:
          1. 'runtime' annotation set by the user at task submission
          2. Historical average from scheduler.task_metadata (if available)
          3. default_runtime fallback
        """
        user_hints = annotations.get("runtime", {})
        runtimes = {}
        for key in tasks:
            if key in user_hints:
                runtimes[key] = float(user_hints[key])
            else:
                # scheduler.task_metadata holds profiling history if available
                meta = getattr(scheduler, "task_metadata", {}).get(key, {})
                runtimes[key] = float(meta.get("runtime", self.default_runtime))
        return runtimes

    # ──────────────────────────────────────────────
    # Slack computation
    # ──────────────────────────────────────────────

    def _compute_slack(
        self,
        dag: dict[str, list[str]],
        runtimes: dict[str, float],
        deadline: float,
    ) -> dict[str, float]:
        earliest = self._earliest_start(dag, runtimes)
        latest = self._latest_start(dag, runtimes, deadline, earliest)
        return {t: latest[t] - earliest[t] for t in dag}

    def _earliest_start(
        self,
        dag: dict[str, list[str]],
        runtimes: dict[str, float],
    ) -> dict[str, float]:
        es: dict[str, float] = {}
        for task in self._topological_sort(dag):
            deps = dag.get(task, [])
            es[task] = max(
                (es[d] + runtimes.get(d, self.default_runtime) for d in deps),
                default=0.0,
            )
        return es

    def _latest_start(
        self,
        dag: dict[str, list[str]],
        runtimes: dict[str, float],
        deadline: float,
        earliest: dict[str, float],
    ) -> dict[str, float]:
        ls: dict[str, float] = {}
        for task in reversed(self._topological_sort(dag)):
            successors = [t for t, deps in dag.items() if task in deps]
            if not successors:
                ls[task] = deadline - runtimes.get(task, self.default_runtime)
            else:
                ls[task] = min(ls[s] for s in successors) - runtimes.get(
                    task, self.default_runtime
                )
        return ls

    def _topological_sort(self, dag: dict[str, list[str]]) -> list[str]:
        visited: set[str] = set()
        order: list[str] = []

        def visit(t: str) -> None:
            if t in visited:
                return
            visited.add(t)
            for dep in dag.get(t, []):
                visit(dep)
            order.append(t)

        for t in dag:
            visit(t)
        return order

    # ──────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────

    def _elapsed(self) -> float:
        if self.start_wall_time is None:
            return 0.0
        return time.time() - self.start_wall_time

    def _workers_with(self, scheduler: Any, pool: str) -> set[str]:
        return {
            addr for addr, w in scheduler.workers.items() if pool in (w.resources or {})
        }

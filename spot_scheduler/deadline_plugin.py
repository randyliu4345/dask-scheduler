from __future__ import annotations

import json
import logging
import pathlib
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from distributed import Scheduler
from distributed.diagnostics.plugin import SchedulerPlugin

from spot_scheduler.assignment import (
    InstanceType,
    assign_instances,
    assign_ondemand,
    max_parallelism,
    simulate_run,
)

logger = logging.getLogger("spot_scheduler")


@dataclass
class SchedulerStats:
    preemptions:         int              = 0
    promotions:          int              = 0
    tasks_completed:     int              = 0
    tasks_on_spot:       int              = 0
    tasks_on_ondemand:   int              = 0
    preempted_tasks:     list[str]        = field(default_factory=list)
    promoted_tasks:      list[str]        = field(default_factory=list)
    task_runtimes:       dict[str, float] = field(default_factory=dict)

    def report(self) -> str:
        total = self.tasks_completed or 1
        return (
            f"completed={self.tasks_completed}"
            f"  spot={self.tasks_on_spot} ({self.tasks_on_spot/total:.0%})"
            f"  ondemand={self.tasks_on_ondemand}"
            f"  preemptions={self.preemptions}"
            f"  promotions={self.promotions}"
            + (f"  preempted={self.preempted_tasks}" if self.preempted_tasks else "")
            + (f"  promoted={self.promoted_tasks}"   if self.promoted_tasks   else "")
        )


class DeadlineSchedulerPlugin(SchedulerPlugin):

    name = "deadline-scheduler"

    def __init__(
        self,
        deadline: float,
        spot_interrupt_buffer: float = 0.2,
        default_runtime: float = 1.0,
        profile_path: str = "runtimes.json",
    ) -> None:
        self.deadline = deadline
        self.spot_interrupt_buffer = spot_interrupt_buffer
        self.default_runtime = default_runtime
        self.profile_path = profile_path

        self.start_wall_time: float | None = None
        self.assignment: dict[str, InstanceType] = {}
        self.scheduler: Optional[Scheduler] = None
        self.stats = SchedulerStats()

        self._runtimes: dict[str, float] = self._load_profile()
        self._task_start: dict[str, float] = {}
        self._remaining: set[str] = set()
        self.should_profile = True

    def _load_profile(self) -> dict[str, float]:
        p = pathlib.Path(self.profile_path)
        if p.exists():
            self.should_profile = False
            logger.info("Loaded runtime profile from %s", p)
            return json.loads(p.read_text())
        return {}

    def _save_profile(self) -> None:
        pathlib.Path(self.profile_path).write_text(json.dumps(self._runtimes, indent=2))
        logger.debug("Saved runtime profile (%d tasks)", len(self._runtimes))

    def start(self, scheduler: Scheduler) -> None:
        self.scheduler = scheduler
        self.start_wall_time = time.time()

    # ──────────────────────────────────────────────
    # Graph arrival
    # ──────────────────────────────────────────────

    def _remaining_dag(
        self, dag: dict[str, list], runtimes: dict[str, float]
    ) -> tuple[dict[str, list], dict[str, float]]:
        remaining_dag = {
            task: [p for p in parents if p in self._remaining]
            for task, parents in dag.items()
            if task in self._remaining
        }
        remaining_runtimes = {
            task: runtime for task, runtime in runtimes.items() if task in remaining_dag
        }
        return remaining_dag, remaining_runtimes

    def update_restrictions(self):
        if self.scheduler is None:
            return
        spot_ws = self._workers_with(self.scheduler, "spot")
        od_ws   = self._workers_with(self.scheduler, "ondemand")
        for task, instance_type in self.assignment.items():
            self.scheduler.set_restrictions(
                {task: spot_ws if instance_type == "spot" else od_ws}
            )

    def reassign_remaining(self):
        remaining_deadline = self.deadline - self._elapsed()
        dag      = self._extract_dag(self.scheduler, list(self._remaining))
        runtimes = self._extract_runtimes(self.scheduler, list(self._remaining), {})

        if runtimes:
            self.assignment = assign_instances(dag, runtimes, remaining_deadline, 1.0)
        else:
            self.assignment = assign_ondemand(dag)

        logger.debug("UPDATED ASSIGNMENTS")
        for key in self.assignment:
            logger.debug("%s → %s", key, self.assignment[key])

        self.update_restrictions()

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
        elapsed            = self._elapsed()
        remaining_deadline = self.deadline - elapsed

        dag      = self._extract_dag(scheduler, tasks)
        runtimes = self._extract_runtimes(scheduler, tasks, annotations)
        dag, runtimes = self._remaining_dag(dag, runtimes)

        if runtimes:
            for num_workers in range(1, max_parallelism(dag) + 1):
                runtime = simulate_run(
                    dag, runtimes, {**self.assignment},
                    {"ondemand": num_workers}, self.deadline,
                )
                logger.debug("%s ondemand workers: %s runtime", num_workers, runtime)

        for key in tasks:
            self._remaining.add(key)

        self.reassign_remaining()

    async def before_close(self) -> None:
        logger.info("Final stats: %s", self.stats.report())
        if self.should_profile:
            self._save_profile()

    # ──────────────────────────────────────────────
    # Task transitions
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

        # record start time
        if start == "waiting" and finish == "processing":
            self._remaining.discard(key)
            self._task_start[key] = time.time()

        # record actual runtime on completion
        elif finish == "memory" and key in self._task_start:
            elapsed = time.time() - self._task_start.pop(key)
            prev    = self._runtimes.get(key, elapsed)
            self._runtimes[key] = 0.8 * prev + 0.2 * elapsed
            self.stats.task_runtimes[key] = elapsed
            self.stats.tasks_completed   += 1

            pool = self.assignment.get(key, "ondemand")
            if pool == "spot":
                self.stats.tasks_on_spot     += 1
            else:
                self.stats.tasks_on_ondemand += 1

            logger.debug("Stats: %s", self.stats.report())

            # backup data on on-demand
            if self.assignment.get(key) == "spot":
                has_ws = self.scheduler.tasks[key].who_has
                od_ws  = self._workers_with(self.scheduler, "ondemand")
                if has_ws is not None and len(has_ws):
                    logger.debug("Backing up computed %s", key)
                    self.scheduler.loop.run_sync(
                        self.scheduler._rebalance_move_data(
                            [(has_ws[0], od_ws[0], self.scheduler.tasks[key])],
                            "backup_spot_result",
                        )
                    )

        # preemption: task lost due to worker death
        elif (
            start == "memory"
            and finish == "released"
            and "handle-worker-cleanup" in stimulus_id
        ):
            self.stats.preemptions += 1
            self.stats.preempted_tasks.append(key)
            logger.info("Preempted: %s (total=%d)", key, self.stats.preemptions)

            if self.assignment.get(key) == "spot":
                self._remaining.add(key)

        self.reassign_remaining()

    def remove_worker(
        self, scheduler: Scheduler, worker: str, *, stimulus_id: str, **kwargs: Any
    ) -> None:
        spot_ws = self._workers_with(scheduler, "spot")
        if worker in spot_ws:
            spot_ws.remove(worker)
        for task, instance_type in self.assignment.items():
            if task not in scheduler.tasks:
                continue
            if instance_type == "spot":
                scheduler.set_restrictions({task: spot_ws})

    def add_worker(self, scheduler: Scheduler, worker: str) -> None:
        spot_ws = self._workers_with(scheduler, "spot")
        if worker not in spot_ws:
            spot_ws.add(worker)
        for task, instance_type in self.assignment.items():
            if task not in scheduler.tasks:
                continue
            if instance_type == "spot":
                scheduler.set_restrictions({task: spot_ws})

    # ──────────────────────────────────────────────
    # Extract DAG structure from scheduler task state
    # ──────────────────────────────────────────────

    def _extract_dag(self, scheduler: Any, tasks: list) -> dict[str, list[str]]:
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
        user_hints = annotations.get("runtime", {})
        runtimes   = {}
        for key in tasks:
            if key in user_hints:
                runtimes[key] = float(user_hints[key])
            elif key in self._runtimes:
                runtimes[key] = self._runtimes[key]
            else:
                return {}
        return runtimes

    # ──────────────────────────────────────────────
    # Slack computation
    # ──────────────────────────────────────────────

    def _compute_slack(self, dag, runtimes, deadline):
        earliest = self._earliest_start(dag, runtimes)
        latest   = self._latest_start(dag, runtimes, deadline, earliest)
        return {t: latest[t] - earliest[t] for t in dag}

    def _earliest_start(self, dag, runtimes):
        es = {}
        for task in self._topological_sort(dag):
            es[task] = max(
                (es[d] + runtimes.get(d, self.default_runtime) for d in dag.get(task, [])),
                default=0.0,
            )
        return es

    def _latest_start(self, dag, runtimes, deadline, earliest):
        ls = {}
        for task in reversed(self._topological_sort(dag)):
            successors = [t for t, deps in dag.items() if task in deps]
            if not successors:
                ls[task] = deadline - runtimes.get(task, self.default_runtime)
            else:
                ls[task] = min(ls[s] for s in successors) - runtimes.get(task, self.default_runtime)
        return ls

    def _topological_sort(self, dag):
        visited, order = set(), []

        def visit(t):
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
            addr for addr, w in scheduler.workers.items()
            if pool in (w.resources or {})
        }
"""
Plugin for dynamic re-scheduling: monitors task completion times and detects
spot instance interruptions to trigger re-computation of assignments.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional, Set

from distributed.diagnostics.plugin import SchedulerPlugin

logger = logging.getLogger("spot_scheduler.rescheduling_plugin")


class DynamicReschedulingPlugin(SchedulerPlugin):
    """Monitor task execution and detect spot interruptions.

    Tracks:
    - Actual completion times for completed tasks
    - Tasks that fail due to spot interruptions
    - Current wall-clock time

    This information is used to recompute slack and reassign remaining tasks.
    """

    name = "spot-scheduler-rescheduler"

    def __init__(
        self,
        initial_assignment: Dict[str, str],
        initial_runtimes: Dict[str, float],
        deadline: float,
        spot_interrupt_buffer: float = 0.2,
    ) -> None:
        """Initialize the rescheduling plugin.

        Parameters
        ----------
        initial_assignment : dict[str, str]
            Initial assignment of tasks to "spot" or "on-demand".
        initial_runtimes : dict[str, float]
            Profiled runtimes for all tasks.
        deadline : float
            Overall deadline in seconds.
        spot_interrupt_buffer : float
            Buffer factor for spot assignment.
        """
        self.initial_assignment = initial_assignment
        self.initial_runtimes = initial_runtimes
        self.deadline = deadline
        self.spot_interrupt_buffer = spot_interrupt_buffer

        # Track actual execution
        self.start_times: Dict[str, float] = {}
        self.completion_times: Dict[str, float] = {}
        self.actual_runtimes: Dict[str, float] = {}
        self.interrupted_tasks: Set[str] = set()
        self.start_wall_time: Optional[float] = None
        self.scheduler: Optional[Any] = None

    def start(self, scheduler: Any) -> None:
        """Record when the scheduler starts."""
        self.start_wall_time = time.time()
        logger.debug("DynamicReschedulingPlugin active")
        self.scheduler = scheduler

    def transition(
        self,
        key: str,
        start: str,
        finish: str,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Track task state transitions."""
        current_time = time.time()

        logger.debug("TRANSITION: %s %s %s", key, start, finish)

        # Track when task starts processing/executing
        if finish in ("processing", "executing"):
            if key not in self.start_times:
                self.start_times[key] = current_time

        if finish == "memory" and key in self.start_times:
            # Task completed successfully
            start_time = self.start_times.pop(key)
            actual_runtime = current_time - start_time
            self.completion_times[key] = current_time
            self.actual_runtimes[key] = actual_runtime

        elif finish in ("error", "cancelled", "forgotten"):
            # Check if this was a spot task that failed
            if key in self.initial_assignment and self.initial_assignment[key] == "spot":
                # Task was interrupted - mark it regardless of start state
                if key not in self.interrupted_tasks:
                    self.interrupted_tasks.add(key)
                    logger.debug(
                        "Spot task %s interrupted (transition: %s -> %s)",
                        key,
                        start,
                        finish,
                    )
                # Remove from start_times since it failed
                if key in self.start_times:
                    self.start_times.pop(key, None)

    def get_elapsed_time(self) -> float:
        """Get elapsed wall-clock time since start."""
        if self.start_wall_time is None:
            return 0.0
        return time.time() - self.start_wall_time

    def get_completed_tasks(self) -> Set[str]:
        """Return set of tasks that have completed."""
        return set(self.completion_times.keys())

    def get_interrupted_tasks(self) -> Set[str]:
        """Return set of tasks that were interrupted."""
        return self.interrupted_tasks.copy()

    def get_actual_runtimes(self) -> Dict[str, float]:
        """Return actual runtimes for completed tasks."""
        return self.actual_runtimes.copy()

    def get_completion_times(self) -> Dict[str, float]:
        """Return completion times (wall-clock) for completed tasks."""
        return self.completion_times.copy()

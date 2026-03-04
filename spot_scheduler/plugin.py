"""
Dask SchedulerPlugin that profiles task runtimes and records them
so they can be used for critical-path analysis on subsequent runs.
"""

from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

from distributed.diagnostics.plugin import SchedulerPlugin

logger = logging.getLogger("spot_scheduler.plugin")


class ProfilingPlugin(SchedulerPlugin):
    """Record wall-clock runtimes for every task that transitions to *memory*.

    After the computation completes (or when :meth:`save` is called),
    the runtimes are written to a JSON file so that the next run can
    use them for critical-path / slack analysis.

    Parameters
    ----------
    profile_path : str or Path
        Where to persist the runtime profile (default ``runtimes.json``).
    """

    name = "spot-scheduler-profiler"

    def __init__(self, profile_path: str | Path = "runtimes.json") -> None:
        self.profile_path = Path(profile_path)
        self.runtimes: Dict[str, float] = {}
        # startstops is populated by the scheduler; we also keep our own
        self._start_times: Dict[str, float] = {}

    # -- lifecycle ------------------------------------------------------------

    def start(self, scheduler: Any) -> None:
        logger.info(
            "ProfilingPlugin active – runtimes will be saved to %s",
            self.profile_path,
        )

    def transition(
        self,
        key: str,
        start: str,
        finish: str,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Track when tasks begin processing and when they finish."""
        if start == "processing":
            # Guard: some transitions skip 'processing'
            pass

        if finish == "processing":
            self._start_times[key] = time.time()

        if finish == "memory" and key in self._start_times:
            elapsed = time.time() - self._start_times.pop(key)
            self.runtimes[key] = elapsed
            logger.debug("Task %s completed in %.3fs", key, elapsed)

        # Also try to use scheduler-provided startstops if available
        startstops = kwargs.get("startstops")
        if startstops and finish == "memory":
            # startstops is a list of (start_time, stop_time) pairs
            total = sum(stop - start for start, stop in startstops)
            self.runtimes[key] = total

    def close(self) -> None:
        """Persist runtimes when the scheduler shuts down."""
        self.save()

    # -- helpers --------------------------------------------------------------

    def save(self) -> None:
        """Write collected runtimes to disk."""
        if not self.runtimes:
            logger.warning("No runtimes recorded – nothing to save.")
            return
        self.profile_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.profile_path, "w") as f:
            json.dump(self.runtimes, f, indent=2)
        logger.info(
            "Saved runtimes for %d tasks to %s",
            len(self.runtimes),
            self.profile_path,
        )

    @staticmethod
    def load(profile_path: str | Path = "runtimes.json") -> Dict[str, float]:
        """Load a previously saved runtime profile."""
        path = Path(profile_path)
        if not path.exists():
            raise FileNotFoundError(
                f"No runtime profile found at {path}. "
                "Run a profiling pass first."
            )
        with open(path) as f:
            return json.load(f)

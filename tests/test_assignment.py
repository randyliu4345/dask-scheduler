"""Tests for spot_scheduler.assignment – spot/on-demand heuristic."""

import pytest

from spot_scheduler.assignment import assign_instances
from spot_scheduler.dag import earliest_finish_times

SIMPLE_DAG = {
    "A": [],
    "B": ["A"],
    "C": ["A"],
    "D": ["B", "C"],
    "E": ["D"],
}

SIMPLE_RUNTIMES = {
    "A": 2.0,
    "B": 4.0,
    "C": 1.5,
    "D": 3.0,
    "E": 2.5,
}


class TestAssignment:
    def test_tight_deadline_critical_path_ondemand(self):
        """With a deadline equal to the makespan, critical-path tasks must be on-demand."""
        eft = earliest_finish_times(SIMPLE_DAG, SIMPLE_RUNTIMES)
        makespan = max(eft.values())
        result = assign_instances(SIMPLE_DAG, SIMPLE_RUNTIMES, makespan)
        # Critical path is A → B → D → E — all should be on-demand
        for task in ["A", "B", "D", "E"]:
            assert result[task] == "on-demand", f"{task} should be on-demand"
        # C is off the critical path, so it may be spot even at makespan
        assert result["C"] in ("spot", "on-demand")

    def test_generous_deadline_has_spot(self):
        """With a very large deadline, non-critical tasks get spot."""
        result = assign_instances(SIMPLE_DAG, SIMPLE_RUNTIMES, 1_000_000)
        assert "spot" in result.values()

    def test_all_tasks_assigned(self):
        result = assign_instances(SIMPLE_DAG, SIMPLE_RUNTIMES, 30.0)
        assert set(result.keys()) == set(SIMPLE_DAG.keys())

    def test_values_are_valid(self):
        result = assign_instances(SIMPLE_DAG, SIMPLE_RUNTIMES, 30.0)
        for v in result.values():
            assert v in ("spot", "on-demand")

    def test_high_buffer_pushes_to_ondemand(self):
        """A very high buffer factor should push most tasks to on-demand."""
        result = assign_instances(
            SIMPLE_DAG, SIMPLE_RUNTIMES, 30.0, spot_interrupt_buffer=100.0
        )
        od_count = sum(1 for v in result.values() if v == "on-demand")
        assert od_count >= 4  # most or all should be on-demand

    def test_zero_buffer_maximises_spot(self):
        """With buffer=0 and generous deadline, everything except zero-slack
        tasks should be spot."""
        result = assign_instances(
            SIMPLE_DAG, SIMPLE_RUNTIMES, 1_000_000, spot_interrupt_buffer=0.0
        )
        assert all(v == "spot" for v in result.values())

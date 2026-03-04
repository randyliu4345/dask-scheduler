"""Tests for spot_scheduler.dag – topological sort, critical path, slack."""

import pytest

from spot_scheduler.dag import (
    compute_slack,
    critical_path,
    earliest_finish_times,
    latest_start_times,
    topo_sort,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Topological sort
# ---------------------------------------------------------------------------


class TestTopoSort:
    def test_simple_dag(self):
        order = topo_sort(SIMPLE_DAG)
        assert set(order) == set(SIMPLE_DAG)
        # Every task must appear after all its dependencies
        for task, deps in SIMPLE_DAG.items():
            for dep in deps:
                assert order.index(dep) < order.index(task)

    def test_single_node(self):
        assert topo_sort({"X": []}) == ["X"]

    def test_linear_chain(self):
        dag = {"A": [], "B": ["A"], "C": ["B"]}
        assert topo_sort(dag) == ["A", "B", "C"]

    def test_cycle_raises(self):
        dag = {"A": ["B"], "B": ["A"]}
        with pytest.raises(ValueError, match="cycle"):
            topo_sort(dag)

    def test_diamond(self):
        dag = {"A": [], "B": ["A"], "C": ["A"], "D": ["B", "C"]}
        order = topo_sort(dag)
        assert order[0] == "A"
        assert order[-1] == "D"


# ---------------------------------------------------------------------------
# Earliest finish times
# ---------------------------------------------------------------------------


class TestEarliestFinish:
    def test_root_task(self):
        eft = earliest_finish_times(SIMPLE_DAG, SIMPLE_RUNTIMES)
        assert eft["A"] == pytest.approx(2.0)

    def test_chain_accumulates(self):
        eft = earliest_finish_times(SIMPLE_DAG, SIMPLE_RUNTIMES)
        # A(2) → B(4) → D(3) → E(2.5)  total = 11.5
        assert eft["E"] == pytest.approx(11.5)

    def test_parallel_takes_max(self):
        eft = earliest_finish_times(SIMPLE_DAG, SIMPLE_RUNTIMES)
        # D starts after max(EFT(B)=6, EFT(C)=3.5) = 6
        assert eft["D"] == pytest.approx(9.0)


# ---------------------------------------------------------------------------
# Latest start times
# ---------------------------------------------------------------------------


class TestLatestStart:
    def test_leaf_task(self):
        deadline = 20.0
        lst = latest_start_times(SIMPLE_DAG, SIMPLE_RUNTIMES, deadline)
        # E is a leaf: LST = 20 - 2.5 = 17.5
        assert lst["E"] == pytest.approx(17.5)

    def test_root_has_most_slack(self):
        deadline = 20.0
        lst = latest_start_times(SIMPLE_DAG, SIMPLE_RUNTIMES, deadline)
        # Root C has the most slack (short runtime, not on critical path)
        slack_c = lst["C"] - (earliest_finish_times(SIMPLE_DAG, SIMPLE_RUNTIMES)["C"] - SIMPLE_RUNTIMES["C"])
        slack_a = lst["A"] - 0.0
        assert slack_c > slack_a


# ---------------------------------------------------------------------------
# Slack
# ---------------------------------------------------------------------------


class TestSlack:
    def test_critical_path_zero_slack(self):
        eft = earliest_finish_times(SIMPLE_DAG, SIMPLE_RUNTIMES)
        makespan = max(eft.values())
        slack = compute_slack(SIMPLE_DAG, SIMPLE_RUNTIMES, makespan)
        # Critical path tasks should have ~zero slack
        cp = critical_path(SIMPLE_DAG, SIMPLE_RUNTIMES)
        for task in cp:
            assert slack[task] == pytest.approx(0.0, abs=1e-9)

    def test_non_critical_positive_slack(self):
        eft = earliest_finish_times(SIMPLE_DAG, SIMPLE_RUNTIMES)
        makespan = max(eft.values())
        slack = compute_slack(SIMPLE_DAG, SIMPLE_RUNTIMES, makespan)
        # C is not on critical path → positive slack
        assert slack["C"] > 0

    def test_generous_deadline_all_positive(self):
        slack = compute_slack(SIMPLE_DAG, SIMPLE_RUNTIMES, 1_000_000)
        assert all(s > 0 for s in slack.values())


# ---------------------------------------------------------------------------
# Critical path
# ---------------------------------------------------------------------------


class TestCriticalPath:
    def test_simple_dag(self):
        cp = critical_path(SIMPLE_DAG, SIMPLE_RUNTIMES)
        # The critical path should be A → B → D → E  (longest chain = 11.5)
        assert cp == ["A", "B", "D", "E"]

    def test_single_task(self):
        assert critical_path({"X": []}, {"X": 5.0}) == ["X"]

    def test_parallel_independent(self):
        dag = {"A": [], "B": []}
        runtimes = {"A": 3.0, "B": 1.0}
        cp = critical_path(dag, runtimes)
        assert cp == ["A"]  # A is longer

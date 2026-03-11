from __future__ import annotations

import math
import random
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

try:
    import matplotlib.pyplot as plt
    import networkx as nx

    _HAS_GRAPH_DEPS = True
except ImportError:
    _HAS_GRAPH_DEPS = False


NUM_TASKS = 10
MIN_FANOUT = 1
MAX_FANOUT = 3
CONNECTIVITY = 0.15
MIN_DURATION_MIN = 360.0
MAX_DURATION_MIN = 1280.0
SEED: Optional[int] = 1001
VERBOSE = True
SHOW_DAG_TREE = True
SHOW_DAG_GRAPH = True
DAG_GRAPH_PATH = "evaluation_dag.png"

RATIO = 0.1
ODDS_OF_FAIL_PER_HOUR = 2.0 / 24.0

NUM_DAGS = 10
NUM_TRIALS_PER_DAG = 10000
HISTOGRAM_PATH = "evaluation_penalty_histogram.png"

PRINT_FAILURES = True
FAILURE_PRINT_LIMIT = 200

SWEEP_ODDS = True
ODDS_SWEEP_MIN_PER_DAY = 1
ODDS_SWEEP_MAX_PER_DAY = 12
ODDS_SWEEP_STEP_PER_DAY = 1
ODDS_SWEEP_PLOT_PATH = "evaluation_penalty_vs_odds.png"

@dataclass
class DagParams:
    num_tasks: int
    min_fanout: int
    max_fanout: int
    connectivity: float
    min_duration_min: float
    max_duration_min: float
    seed: Optional[int]

@dataclass
class Task:
    id: int
    duration_min: float
    predecessors: list[int] = field(default_factory=list)
    successors: list[int] = field(default_factory=list)
    slack_min: Optional[float] = None
    slack_used_min: Optional[float] = None

    def __repr__(self) -> str:
        return f"Task(id={self.id}, duration={self.duration_min:.1f}m)"


class TaskDag:
    def __init__(self) -> None:
        self.tasks: dict[int, Task] = {}
        self._start_id: Optional[int] = None
        self._topo_order: Optional[list[int]] = None

    def add_task(self, task_id: int, duration_min: float) -> Task:
        t = Task(id=task_id, duration_min=duration_min)
        self.tasks[task_id] = t
        return t

    def add_edge(self, from_id: int, to_id: int) -> None:
        self.tasks[from_id].successors.append(to_id)
        self.tasks[to_id].predecessors.append(from_id)

    def set_start(self, task_id: int) -> None:
        self._start_id = task_id
        self._topo_order = None

    @property
    def start_id(self) -> int:
        if self._start_id is None:
            raise ValueError("Start node not set")
        return self._start_id

    def get_topo_order(self) -> list[int]:
        if self._topo_order is not None:
            return self._topo_order
        in_degree = {tid: 0 for tid in self.tasks}
        for t in self.tasks.values():
            for s in t.successors:
                in_degree[s] += 1
        q: deque[int] = deque(tid for tid, d in in_degree.items() if d == 0)
        order: list[int] = []
        while q:
            u = q.popleft()
            order.append(u)
            for v in self.tasks[u].successors:
                in_degree[v] -= 1
                if in_degree[v] == 0:
                    q.append(v)
        if len(order) != len(self.tasks):
            raise ValueError("Graph has a cycle")
        self._topo_order = order
        return order

    def end_ids(self) -> list[int]:
        return [tid for tid, t in self.tasks.items() if not t.successors]


def build_random_dag(params: DagParams) -> TaskDag:
    rng = random.Random(params.seed)
    g = TaskDag()

    for i in range(params.num_tasks):
        dur = rng.uniform(params.min_duration_min, params.max_duration_min)
        g.add_task(i, dur)

    n = params.num_tasks
    possible_edges = [
        (u, v) for u in range(n) for v in range(u + 1, n)
    ]
    rng.shuffle(possible_edges)
    target_edges = max(0, int(len(possible_edges) * params.connectivity))
    target_edges = max(target_edges, n - 1)

    added = 0
    for u, v in possible_edges:
        if added >= target_edges:
            break
        g.add_edge(u, v)
        added += 1

    for v in range(1, n):
        if not g.tasks[v].predecessors:
            u = rng.randint(0, v - 1)
            if v not in g.tasks[u].successors:
                g.add_edge(u, v)

    for u in range(n):
        t = g.tasks[u]
        current = len(t.successors)
        need = max(0, params.min_fanout - current)
        cap = max(0, params.max_fanout - current)
        if need > 0 or (cap > 0 and rng.random() < 0.5):
            candidates = [v for v in range(u + 1, n) if v not in t.successors]
            rng.shuffle(candidates)
            k = min(len(candidates), need + rng.randint(0, cap) if cap else need)
            for v in candidates[:k]:
                g.add_edge(u, v)

    g.set_start(0)
    return g


def critical_path(g: TaskDag) -> tuple[float, list[int], set[int]]:
    order = g.get_topo_order()
    start = g.start_id
    dist: dict[int, float] = {}
    pred: dict[int, Optional[int]] = {}

    for u in order:
        if u == start:
            dist[u] = g.tasks[u].duration_min
            pred[u] = None
        else:
            best = -1.0
            best_p: Optional[int] = None
            for p in g.tasks[u].predecessors:
                if p in dist and dist[p] + g.tasks[u].duration_min > best:
                    best = dist[p] + g.tasks[u].duration_min
                    best_p = p
            dist[u] = best
            pred[u] = best_p

    end_ids = g.end_ids()
    if not end_ids:
        raise ValueError("DAG has no end nodes")
    max_dist = max(dist.get(e, -1.0) for e in end_ids)
    critical_endpoints = {e for e in end_ids if dist.get(e, -1.0) == max_dist}
    best_end = next(iter(critical_endpoints))
    total = dist[best_end]

    path: list[int] = []
    cur: Optional[int] = best_end
    while cur is not None:
        path.append(cur)
        cur = pred[cur]
    path.reverse()
    return total, path, critical_endpoints


def compute_slack(g: TaskDag, project_end_min: float) -> dict[int, float]:
    order = g.get_topo_order()
    start = g.start_id
    est: dict[int, float] = {}
    for n in order:
        if n == start:
            est[n] = 0.0
        else:
            est[n] = max(
                est[p] + g.tasks[p].duration_min for p in g.tasks[n].predecessors
            )
    lft: dict[int, float] = {}
    for n in reversed(order):
        if not g.tasks[n].successors:
            lft[n] = project_end_min
        else:
            lft[n] = min(
                lft[s] - g.tasks[s].duration_min for s in g.tasks[n].successors
            )
    lst = {n: lft[n] - g.tasks[n].duration_min for n in g.tasks}
    slack = {n: lst[n] - est[n] for n in g.tasks}
    for n, s in slack.items():
        g.tasks[n].slack_min = s
    return slack


def compute_slack_usage(g: TaskDag) -> dict[int, float]:
    order = g.get_topo_order()
    used: dict[int, float] = {}
    for n in reversed(order):
        t = g.tasks[n]
        s = t.slack_min or 0.0
        succ_slacks = [(g.tasks[sid].slack_min or 0.0) for sid in t.successors]
        max_succ_slack = max(succ_slacks) if succ_slacks else 0.0
        val = max(0.0, s - max_succ_slack)
        used[n] = val
        t.slack_used_min = val
    return used


def simulate_failures(
    g: TaskDag,
    ratio_threshold: float,
    odds_per_hour: float,
    rng: random.Random,
    print_state: Optional[list[int]] = None,
) -> tuple[float, float]:
    total_penalty = 0.0
    max_single_penalty = 0.0
    failed_spot_task_ids: set[int] = set()
    for t in g.tasks.values():
        if not t.slack_used_min or t.duration_min <= 0.0:
            continue

        ratio = t.slack_used_min / t.duration_min
        if ratio <= ratio_threshold:
            t.slack_used_min = 0.0
            continue

        duration_hours = t.duration_min / 60.0
        lam = odds_per_hour * duration_hours
        p_fail = 1.0 - math.exp(-lam) if lam > 0.0 else 0.0
        if rng.random() < p_fail:
            failed_spot_task_ids.add(t.id)
            base_delay = rng.uniform(0.0, t.duration_min)
            slack_here = t.slack_min or 0.0
            penalty = max(0.0, base_delay - slack_here)
            if print_state is not None and print_state[0] > 0:
                print(f"Task {t.id} failed: penalty = {penalty:.2f} min")
                print_state[0] -= 1
            total_penalty += penalty
            max_single_penalty = max(max_single_penalty, penalty)

    return total_penalty, max_single_penalty, failed_spot_task_ids


def print_dag_tree(
    g: TaskDag,
    critical_path_ids: Optional[set[int]] = None,
) -> None:
    start = g.start_id
    display_parent: dict[int, Optional[int]] = {}
    display_parent[start] = None
    q: deque[int] = deque([start])
    while q:
        u = q.popleft()
        for v in g.tasks[u].successors:
            if v not in display_parent:
                display_parent[v] = u
                q.append(v)

    display_children: dict[int, list[int]] = {tid: [] for tid in g.tasks}
    for tid, par in display_parent.items():
        if par is not None:
            display_children[par].append(tid)

    critical_path_ids = critical_path_ids or set()

    def visit(node: int, prefix: str, is_last: bool, is_root: bool = False) -> None:
        t = g.tasks[node]
        if is_root:
            branch = ""
        else:
            branch = "\\-- " if is_last else "+-- "
        extra = " [CRITICAL]" if node in critical_path_ids else ""
        others = [p for p in t.predecessors if p != display_parent.get(node)]
        also = f"  (also from: {others})" if others else ""
        slack_str = f" slack={t.slack_min:.1f}m" if t.slack_min is not None else ""
        used_str = (
            f" slack_used={t.slack_used_min:.1f}m" if t.slack_used_min is not None else ""
        )
        print(
            f"{prefix}{branch}[{node}] {t.duration_min:.1f}m"
            f"{slack_str}{used_str}{extra}{also}"
        )
        child_prefix = prefix + ("    " if is_last else "|   ") if not is_root else ""
        kids = display_children[node]
        for i, c in enumerate(kids):
            visit(c, child_prefix, i == len(kids) - 1, is_root=False)

    print("Task DAG (tree view, single start -> sinks):")
    print("-" * 50)
    visit(start, "", True, is_root=True)


def _draw_dag_graph(
    g: TaskDag,
    critical_path_ids: list[int],
    save_path: Optional[str] = "evaluation_dag.png",
) -> None:
    if not _HAS_GRAPH_DEPS:
        print("Skipping graph (install networkx and matplotlib to enable).")
        return
    G = nx.DiGraph()
    for tid, t in g.tasks.items():
        G.add_node(tid, duration=t.duration_min)
    for tid, t in g.tasks.items():
        for s in t.successors:
            G.add_edge(tid, s)
    cp_set = set(critical_path_ids)
    start = g.start_id
    topo = g.get_topo_order()
    rank: dict[int, int] = {}
    for n in topo:
        if n == start:
            rank[n] = 0
        else:
            rank[n] = 1 + max(rank[p] for p in g.tasks[n].predecessors)
    layers: dict[int, list[int]] = {}
    for n, r in rank.items():
        layers.setdefault(r, []).append(n)
    pos = {}
    layer_height = 1.5
    for r, nodes in sorted(layers.items()):
        for i, n in enumerate(nodes):
            y = -layer_height * (i - (len(nodes) - 1) / 2)
            pos[n] = (r, y)
    node_colors = ["#ff9f43" if n in cp_set else "#74b9ff" for n in G.nodes()]
    cp_edges = set()
    for i in range(len(critical_path_ids) - 1):
        cp_edges.add((critical_path_ids[i], critical_path_ids[i + 1]))
    edge_colors = ["#e17055" if (u, v) in cp_edges else "#b2bec3" for u, v in G.edges()]
    fig, ax = plt.subplots(figsize=(10, 6))
    nx.draw_networkx_nodes(
        G, pos, node_color=node_colors, node_size=3200, ax=ax
    )
    nx.draw_networkx_edges(
        G, pos, edge_color=edge_colors, arrows=True, arrowsize=20, ax=ax
    )
    labels = {}
    for n in G.nodes():
        t = g.tasks[n]
        slack_str = f"\nslack={t.slack_min:.0f}m" if t.slack_min is not None else ""
        used_str = (
            f"\nslack_used={t.slack_used_min:.0f}m"
            if t.slack_used_min is not None
            else ""
        )
        labels[n] = f"{n}\n{t.duration_min:.0f}m{slack_str}{used_str}"
    nx.draw_networkx_labels(G, pos, labels, font_size=16, ax=ax)
    ax.set_title("Task DAG (orange = critical path)")
    ax.axis("off")
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=120)
        print(f"  Saved DAG graph to {save_path}")
    else:
        plt.show()

    
def run_evaluation() -> None:
    base_seed = SEED or 0
    penalty_ratios: list[float] = []  # max_penalty/total_time as percentage
    total_execution_time_min: float = 0.0
    total_spot_instance_time_min: float = 0.0
    print_state = [FAILURE_PRINT_LIMIT] if PRINT_FAILURES else None

    for dag_idx in range(NUM_DAGS):
        params = DagParams(
            num_tasks=NUM_TASKS,
            min_fanout=MIN_FANOUT,
            max_fanout=MAX_FANOUT,
            connectivity=CONNECTIVITY,
            min_duration_min=MIN_DURATION_MIN,
            max_duration_min=MAX_DURATION_MIN,
            seed=base_seed + dag_idx,
        )
        g = build_random_dag(params)
        total_min, path, _ = critical_path(g)
        compute_slack(g, total_min)

        all_nodes_time = sum(t.duration_min for t in g.tasks.values())
        critical_path_set = set(path)
        spot_instance_time = sum(
            t.duration_min for t in g.tasks.values()
            if t.id not in critical_path_set
        )
        total_execution_time_min += all_nodes_time
        total_spot_instance_time_min += spot_instance_time

        if dag_idx == 0:
            compute_slack_usage(g)  # Populate slack_used for graph display
            _draw_dag_graph(g, path, "evaluation/evaluation_dag_1.png")

        for trial_idx in range(NUM_TRIALS_PER_DAG):
            compute_slack_usage(g)  # Reset before each trial (simulate_failures mutates)
            sim_rng = random.Random(base_seed + 12345 + dag_idx * 10000 + trial_idx)
            total_penalty, max_penalty, _ = simulate_failures(
                g,
                ratio_threshold=RATIO,
                odds_per_hour=ODDS_OF_FAIL_PER_HOUR,
                rng=sim_rng,
                print_state=print_state,
            )
            pct = 100.0 * max_penalty / total_min if total_min > 0 else 0.0
            penalty_ratios.append(pct)

        print(f"  DAG {dag_idx + 1}/{NUM_DAGS}: total execution time = {all_nodes_time:.2f} min, "
              f"spot instance time = {spot_instance_time:.2f} min")
        if VERBOSE:
            print(f"    ({len(g.tasks)} tasks, critical path = {total_min:.1f} min)")

    # Save histogram
    if _HAS_GRAPH_DEPS and penalty_ratios:
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.hist(penalty_ratios, bins=50, edgecolor="black", alpha=0.7)
        ax.set_yscale("log")
        ax.set_xlabel("max(penalties) / total time (%)")
        ax.set_ylabel("Count")
        ax.set_title(
            f"Distribution of max single-node penalty as % of total time\n"
            f"({NUM_DAGS} DAGs × {NUM_TRIALS_PER_DAG} trials = {len(penalty_ratios)} samples)"
        )
        plt.tight_layout()
        plt.savefig(HISTOGRAM_PATH, dpi=120)
        print(f"\nSaved histogram to {HISTOGRAM_PATH}")
        plt.close()
    elif not _HAS_GRAPH_DEPS:
        print("Install matplotlib to generate the histogram.")

    # Summary stats
    if penalty_ratios:
        print(f"\nSummary: max(penalties)/total_time (%)")
        print(f"  Min:    {min(penalty_ratios):.2f}%")
        print(f"  Max:    {max(penalty_ratios):.2f}%")
        print(f"  Mean:   {sum(penalty_ratios) / len(penalty_ratios):.2f}%")

    print(f"\nTotal execution time (all nodes summed): {total_execution_time_min:.2f} min")
    print(f"Total time in spot instances:             {total_spot_instance_time_min:.2f} min")


def run_odds_sweep() -> None:
    if not _HAS_GRAPH_DEPS:
        print("Install matplotlib to generate the sweep plot.")
        return

    base_seed = SEED or 0
    per_day_values = list(
        range(
            ODDS_SWEEP_MIN_PER_DAY,
            ODDS_SWEEP_MAX_PER_DAY + 1,
            ODDS_SWEEP_STEP_PER_DAY,
        )
    )
    odds_values = [v / 24.0 for v in per_day_values]
    mean_total_penalties: list[float] = []
    mean_penalty_over_critical_path: list[float] = []
    mean_spot_utilization_pct: list[float] = []
    mean_cost: list[float] = []

    for odds_per_hour in odds_values:
        total_penalty_sum = 0.0
        penalty_over_cp_sum = 0.0
        spot_utilization_sum = 0.0
        cost_sum = 0.0
        trials_count = 0

        for dag_idx in range(NUM_DAGS):
            params = DagParams(
                num_tasks=NUM_TASKS,
                min_fanout=MIN_FANOUT,
                max_fanout=MAX_FANOUT,
                connectivity=CONNECTIVITY,
                min_duration_min=MIN_DURATION_MIN,
                max_duration_min=MAX_DURATION_MIN,
                seed=base_seed + dag_idx,
            )
            g = build_random_dag(params)
            total_min, path, _ = critical_path(g)
            compute_slack(g, total_min)
            critical_path_set = set(path)
            all_nodes_time = sum(t.duration_min for t in g.tasks.values())

            for trial_idx in range(NUM_TRIALS_PER_DAG):
                compute_slack_usage(g)
                sim_rng = random.Random(
                    base_seed
                    + 99999
                    + int(odds_per_hour * 1_000_000)
                    + dag_idx * 10000
                    + trial_idx
                )
                total_penalty, _max_penalty, failed_spot_ids = simulate_failures(
                    g,
                    ratio_threshold=RATIO,
                    odds_per_hour=odds_per_hour,
                    rng=sim_rng,
                    print_state=None,
                )
                total_penalty_sum += total_penalty
                if total_min > 0.0:
                    penalty_over_cp_sum += total_penalty / total_min
                if all_nodes_time > 0.0:
                    spot_success_runtime = sum(
                        t.duration_min for t in g.tasks.values()
                        if t.id not in critical_path_set and t.id not in failed_spot_ids
                    )
                    spot_utilization_sum += 100.0 * spot_success_runtime / all_nodes_time
                    cost = (
                        (all_nodes_time - spot_success_runtime) / all_nodes_time
                        + (spot_success_runtime / all_nodes_time) * 0.1
                    )
                    cost_sum += cost
                trials_count += 1

        mean_total_penalties.append(
            total_penalty_sum / trials_count if trials_count else 0.0
        )
        mean_penalty_over_critical_path.append(
            penalty_over_cp_sum / trials_count if trials_count else 0.0
        )
        mean_spot_utilization_pct.append(
            spot_utilization_sum / trials_count if trials_count else 0.0
        )
        mean_cost.append(cost_sum / trials_count if trials_count else 0.0)
        print(
            f"  Sweep {odds_per_hour*24:.0f}/day: "
            f"mean total penalty = {mean_total_penalties[-1]:.2f} min, "
            f"mean penalty/critical path = {mean_penalty_over_critical_path[-1]:.4f}, "
            f"avg spot utilization = {mean_spot_utilization_pct[-1]:.2f}%, "
            f"avg cost = {mean_cost[-1]:.4f}"
        )

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(per_day_values, mean_total_penalties, marker="o", linewidth=2)
    ax.set_xlabel("Failure rate (events/day)")
    ax.set_ylabel("Mean total penalty (minutes)")
    ax.set_title(
        "Mean total penalty vs failure rate\n"
        f"({NUM_DAGS} DAGs × {NUM_TRIALS_PER_DAG} trials per DAG)"
    )
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(ODDS_SWEEP_PLOT_PATH, dpi=120)
    print(f"\nSaved sweep plot to {ODDS_SWEEP_PLOT_PATH}")
    plt.close()

    ratio_plot_path = ODDS_SWEEP_PLOT_PATH.replace(".png", "_ratio.png")
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(per_day_values, mean_penalty_over_critical_path, marker="o", linewidth=2)
    ax.set_xlabel("Failure rate (events/day)")
    ax.set_ylabel("Mean(total_penalty / critical_path_length)")
    ax.set_title(
        "Mean penalty/critical path vs failure rate\n"
        f"({NUM_DAGS} DAGs × {NUM_TRIALS_PER_DAG} trials per DAG)"
    )
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(ratio_plot_path, dpi=120)
    print(f"Saved sweep ratio plot to {ratio_plot_path}")
    plt.close()

    util_plot_path = ODDS_SWEEP_PLOT_PATH.replace(".png", "_spot_utilization.png")
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(per_day_values, mean_spot_utilization_pct, marker="o", linewidth=2)
    ax.set_xlabel("Failure rate (events/day)")
    ax.set_ylabel("Avg spot utilization (%)")
    ax.set_title(
        "Avg spot utilization vs failure rate\n"
        "(spot_success_runtime / total_runtime_all_nodes; failed spot tasks excluded)"
    )
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(util_plot_path, dpi=120)
    print(f"Saved spot utilization plot to {util_plot_path}")
    plt.close()

    cost_plot_path = ODDS_SWEEP_PLOT_PATH.replace(".png", "_cost.png")
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(per_day_values, mean_cost, marker="o", linewidth=2)
    ax.set_xlabel("Failure rate (events/day)")
    ax.set_ylabel("Avg cost")
    ax.set_title(
        "Avg cost vs failure rate\n"
        "((T - spot_success) / T + (spot_success / T) * 0.1; on-demand=1, spot=0.1)"
    )
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(cost_plot_path, dpi=120)
    print(f"Saved cost plot to {cost_plot_path}")
    plt.close()


def main() -> None:
    if SWEEP_ODDS:
        run_odds_sweep()
        return
    if NUM_DAGS == 1 and NUM_TRIALS_PER_DAG == 1:
        # Original single-DAG behavior
        params = DagParams(
            num_tasks=NUM_TASKS,
            min_fanout=MIN_FANOUT,
            max_fanout=MAX_FANOUT,
            connectivity=CONNECTIVITY,
            min_duration_min=MIN_DURATION_MIN,
            max_duration_min=MAX_DURATION_MIN,
            seed=SEED,
        )
        g = build_random_dag(params)
        total_min, path, _ = critical_path(g)
        compute_slack(g, total_min)
        compute_slack_usage(g)
        sim_rng = random.Random((SEED or 0) + 12345)
        print_state = [FAILURE_PRINT_LIMIT] if PRINT_FAILURES else None
        total_penalty, max_penalty, _ = simulate_failures(
            g,
            ratio_threshold=RATIO,
            odds_per_hour=ODDS_OF_FAIL_PER_HOUR,
            rng=sim_rng,
            print_state=print_state,
        )

        print("DAG summary")
        print("----------")
        print(f"  Tasks: {len(g.tasks)}")
        print(f"  Start: {g.start_id}")
        print(f"  End nodes: {g.end_ids()}")
        print()
        print("Critical path")
        print("-------------")
        print(f"  Length (minutes): {total_min:.2f}")
        print(f"  Path (task ids): {path}")
        critical_path_set = set(path)
        if SHOW_DAG_TREE:
            print()
            print_dag_tree(g, critical_path_ids=critical_path_set)
        if SHOW_DAG_GRAPH:
            _draw_dag_graph(g, path, DAG_GRAPH_PATH)
        if VERBOSE:
            print()
            print("Tasks on critical path:")
            for tid in path:
                t = g.tasks[tid]
                slack_str = f" slack={t.slack_min:.1f}m" if t.slack_min is not None else ""
                used_str = (
                    f" slack_used={t.slack_used_min:.1f}m"
                    if t.slack_used_min is not None
                    else ""
                )
                print(
                    f"  {t}{slack_str}{used_str}  preds={t.predecessors} succs={t.successors}"
                )
            print()
            print(
                "All tasks (topo order, slack = wait time without delaying project):"
            )
            for tid in g.get_topo_order():
                t = g.tasks[tid]
                on_cp = " [CRITICAL]" if tid in critical_path_set else ""
                slack_str = f" slack={t.slack_min:.1f}m" if t.slack_min is not None else ""
                used_str = (
                    f" slack_used={t.slack_used_min:.1f}m"
                    if t.slack_used_min is not None
                    else ""
                )
                print(f"  {t}{slack_str}{used_str}{on_cp}")

        all_nodes_time = sum(t.duration_min for t in g.tasks.values())
        spot_instance_time = sum(
            t.duration_min for t in g.tasks.values()
            if t.id not in critical_path_set
        )
        print()
        print("Simulation summary")
        print("------------------")
        print(f"  Total execution time (all nodes summed): {all_nodes_time:.2f} minutes")
        print(f"  Total time in spot instances: {spot_instance_time:.2f} minutes")
        print(f"  Total penalty (extra time): {total_penalty:.2f} minutes")
        print(f"  Max single-node penalty: {max_penalty:.2f} minutes")
        print(
            f"  Effective critical path length: {total_min + total_penalty:.2f} minutes"
        )
    else:
        run_evaluation()


if __name__ == "__main__":
    main()

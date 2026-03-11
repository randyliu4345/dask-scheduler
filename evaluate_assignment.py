import random
import itertools
from typing import Dict, List

from spot_scheduler.assignment import InstanceType, assign_greedy_cp_increase, assign_greedy_cp_increase_fast, assign_greedy_runtime, assign_ondemand, critical_path_duration, optimal_assignment_pruned
from spot_scheduler.dag import topo_sort


def assignment_spot_duration(
    dag: Dict[str, List[str]],
    runtimes: Dict[str, float],
    assignment: Dict[str, InstanceType],
) -> float:
    """Sum of runtimes of tasks assigned to spot — higher is better (more work on cheap instances)."""
    return sum(runtimes[t] for t, pool in assignment.items() if pool == "spot")

def optimal_assignment(
    dag: Dict[str, List[str]],
    runtimes: Dict[str, float],
    deadline: float,
    spot_interrupt_buffer: float = 1.0,
) -> Dict[str, InstanceType]:
    """
    Enumerate all 2^n assignments, filter those where the critical path
    (using effective runtimes) meets the deadline, return the one with
    the highest spot duration.
    """
    tasks = list(dag.keys())
    best_assignment: Dict[str, InstanceType] | None = None
    best_score = -1.0

    for bits in itertools.product(["spot", "ondemand"], repeat=len(tasks)):
        assignment = dict(zip(tasks, bits))

        effective = {
            t: (
                runtimes[t] * (1 + spot_interrupt_buffer)
                if assignment[t] == "spot"
                else runtimes[t]
            )
            for t in tasks
        }

        if critical_path_duration(dag, effective) <= deadline:
            score = assignment_spot_duration(dag, runtimes, assignment)
            if score > best_score:
                best_score = score
                best_assignment = assignment

    if best_assignment is None:
        best_assignment = {t: "ondemand" for t in tasks}

    return best_assignment

def random_dag(
    n_tasks: int = 10,
    edge_prob: float = 0.3,
    min_runtime: float = 1.0,
    max_runtime: float = 10.0,
    seed: int | None = None,
) -> tuple[Dict[str, List[str]], Dict[str, float]]:
    """
    Generate a random DAG with n_tasks nodes.
    Edges only go from lower-indexed to higher-indexed nodes to guarantee acyclicity.
    """
    rng = random.Random(seed)
    tasks = [chr(ord("A") + i) if n_tasks <= 26 else f"T{i}" for i in range(n_tasks)]

    dag = {t: [] for t in tasks}
    for i, task in enumerate(tasks):
        for j in range(i):  # can only depend on earlier tasks
            if rng.random() < edge_prob:
                dag[task].append(tasks[j])

    runtimes = {t: round(rng.uniform(min_runtime, max_runtime), 2) for t in tasks}
    return dag, runtimes



def evaluate_heuristic(
    heuristic,
    n_trials: int = 500,
    n_tasks: int = 15,
    edge_prob: float = 0.3,
    deadline_slack: float = 1.5,
    spot_interrupt_buffer: float = 1.0,
    seed: int = 42,
) -> dict:
    ratios = []
    misses = 0
    spot_fractions = []
    trials = []

    for i in range(n_trials):
        dag, runtimes = random_dag(n_tasks=n_tasks, edge_prob=edge_prob, seed=seed + i)
        cp = critical_path_duration(dag, runtimes)
        deadline = cp * deadline_slack

        opt_assignment = optimal_assignment(
            dag, runtimes, deadline, spot_interrupt_buffer
        )
        opt_score = assignment_spot_duration(dag, runtimes, opt_assignment)

        heuristic_assignment = heuristic(
            dag, dict(runtimes), deadline, spot_interrupt_buffer
        )

        effective = {
            t: (
                runtimes[t] * (1 + spot_interrupt_buffer)
                if heuristic_assignment[t] == "spot"
                else runtimes[t]
            )
            for t in dag
        }
        missed = critical_path_duration(dag, effective) > deadline
        if missed:
            misses += 1

        heuristic_score = assignment_spot_duration(dag, runtimes, heuristic_assignment)
        total_runtime   = sum(runtimes.values())
        spot_fraction   = heuristic_score / total_runtime if total_runtime > 0 else 0.0
        spot_fractions.append(spot_fraction)

        ratio = (heuristic_score / opt_score) if opt_score > 0 else 1.0
        ratios.append(ratio)
        trials.append({
            "dag":           dag,
            "runtimes":      runtimes,
            "deadline":      deadline,
            "opt":           opt_score,
            "heuristic":     heuristic_score,
            "ratio":         ratio,
            "spot_fraction": spot_fraction,
            "missed":        missed,
        })

    return {
        "mean_ratio":        sum(ratios) / len(ratios),
        "mean_spot_fraction": sum(spot_fractions) / len(spot_fractions),
        "deadline_misses":   misses,
        "n_trials":          n_trials,
        "trials":            trials,
    }

import matplotlib.pyplot as plt
import numpy as np

def plot_results(results: dict[str, dict]) -> None:
    names  = list(results.keys())
    ratios = [r["mean_ratio"]        for r in results.values()]
    spots  = [r["mean_spot_fraction"] for r in results.values()]
    misses = [r["deadline_misses"]   for r in results.values()]
    trials = results[names[0]]["n_trials"]

    x     = np.arange(len(names))
    width = 0.35

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle(f"Assignment Heuristic Comparison ({trials} trials)", fontsize=13)

    # ── left: optimality ratio ──────────────────────────────────────────────
    bars1 = ax1.bar(x, ratios, width=0.5, color=["#2ecc71", "#3498db", "#e67e22", "#e74c3c"])
    ax1.set_xticks(x)
    ax1.set_xticklabels(names, rotation=15, ha="right")
    ax1.set_ylim(0, 1.1)
    ax1.set_ylabel("Mean spot duration / optimal spot duration")
    ax1.set_title("Optimality Ratio (higher = better)")
    ax1.axhline(1.0, color="black", linestyle="--", linewidth=0.8, alpha=0.5)
    for bar, miss in zip(bars1, misses):
        if miss > 0:
            ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.01,
                     f"⚠ {miss} misses", ha="center", va="bottom", fontsize=8, color="red")
        ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() / 2,
                 f"{bar.get_height():.3f}", ha="center", va="center", fontsize=9, color="white", fontweight="bold")

    # ── right: spot percentage ──────────────────────────────────────────────
    bars2 = ax2.bar(x, [s * 100 for s in spots], width=0.5, color=["#2ecc71", "#3498db", "#e67e22", "#e74c3c"])
    ax2.set_xticks(x)
    ax2.set_xticklabels(names, rotation=15, ha="right")
    ax2.set_ylim(0, 110)
    ax2.set_ylabel("% of total task runtime on spot")
    ax2.set_title("Spot Utilisation (higher = cheaper)")
    for bar in bars2:
        ax2.text(bar.get_x() + bar.get_width() / 2, bar.get_height() / 2,
                 f"{bar.get_height():.1f}%", ha="center", va="center", fontsize=9, color="white", fontweight="bold")

    plt.tight_layout()
    plt.savefig("heuristic_comparison.png", dpi=150, bbox_inches="tight")
    plt.show()


if __name__ == "__main__":
    # results_optimal = evaluate_heuristic(optimal_assignment)
    results_optimal_pruned = evaluate_heuristic(optimal_assignment_pruned)
    results_greedy_fast  = evaluate_heuristic(assign_greedy_cp_increase_fast)
    results_runtime = evaluate_heuristic(assign_greedy_runtime)

    all_results = {
        # "optimal":         results_optimal,
        "optimal":         results_optimal_pruned,
        "greedy_critical":       results_greedy_fast,
        "greedy_runtime":  results_runtime,
    }

    for name, r in all_results.items():
        print(
            f"{name:20s}  mean_ratio={r['mean_ratio']:.3f}"
            f"  spot%={r['mean_spot_fraction']:.1%}"
            f"  misses={r['deadline_misses']}/{r['n_trials']}"
        )

    plot_results(all_results)

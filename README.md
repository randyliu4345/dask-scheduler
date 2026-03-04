# spot-scheduler

A Dask scheduler plugin that profiles task runtimes, computes the **critical path** and **slack** on a task DAG, then assigns each task to either a **spot** (cheap, preemptible) or **on-demand** (reliable, expensive) worker pool — minimising cloud cost while meeting a user-defined deadline.

---

## How It Works

```
┌──────────────┐      ┌────────────────────┐      ┌──────────────────┐
│  Profiling   │ ───► │  Critical Path &   │ ───► │ Task Assignment  │
│  Run         │      │  Slack Analysis    │      │ (spot / on-dem.) │
└──────────────┘      └────────────────────┘      └──────────────────┘
       │                        │                          │
  All tasks run           Uses profiled              Tasks with high
  on on-demand,           runtimes + DAG             slack → spot;
  wall-clock times        to compute EST,            tasks on critical
  saved to JSON.          LST, and slack.            path → on-demand.
```

### Two-pass workflow

| Pass | What happens | Workers used |
|------|-------------|--------------|
| **1 — Profiling** | Every task runs on on-demand. Wall-clock runtimes are recorded to `runtimes.json`. | on-demand only |
| **2 — Optimised** | Runtimes are loaded; critical path & slack are computed. Tasks with enough slack are sent to spot workers; critical-path tasks go to on-demand. | spot + on-demand |

### Assignment heuristic

```
if slack(task) > buffer_factor × runtime(task):
    → spot
else:
    → on-demand
```

`buffer_factor` (default **0.2**) absorbs the risk of a spot interruption adding ~20 % delay.

---

## Project Structure

```
spot_scheduler/
├── __init__.py          # Package metadata
├── dag.py               # Topological sort, EFT/LST, slack, critical path
├── assignment.py        # Spot vs. on-demand heuristic
├── plugin.py            # Dask SchedulerPlugin for runtime profiling
└── runner.py            # High-level runner that wires everything together

tests/
├── test_dag.py          # Unit tests for DAG analysis
└── test_assignment.py   # Unit tests for assignment logic

example.py               # End-to-end demo with a synthetic DAG
requirements.txt
pyproject.toml
```

---

## Quick Start

### 1. Install

```bash
pip install -e ".[dev]"
```

Or just install the dependencies:

```bash
pip install -r requirements.txt
```

### 2. Run the example (local cluster)

```bash
# Pass 1 — profiling (records runtimes.json)
python example.py --profile

# Pass 2 — optimised (uses runtimes.json, assigns spot/on-demand)
python example.py --deadline 30
```

### 3. Run on a real cluster

Launch workers with resource tags:

```bash
# Spot fleet
dask-worker tcp://scheduler:8786 --resources "spot=1"

# On-demand fleet
dask-worker tcp://scheduler:8786 --resources "ondemand=1"
```

Then point the example at your scheduler:

```bash
python example.py --scheduler tcp://scheduler:8786 --profile
python example.py --scheduler tcp://scheduler:8786 --deadline 3600
```

---

## API Reference

### `spot_scheduler.dag`

| Function | Description |
|----------|-------------|
| `topo_sort(dag)` | Kahn's algorithm topological sort. Raises `ValueError` on cycles. |
| `earliest_finish_times(dag, runtimes)` | Compute EFT for every task. |
| `latest_start_times(dag, runtimes, deadline)` | Compute LST for every task given a deadline. |
| `compute_slack(dag, runtimes, deadline)` | `slack(t) = LST(t) - EST(t)` for every task. |
| `critical_path(dag, runtimes)` | Return the list of tasks with zero slack (at the makespan deadline). |

### `spot_scheduler.assignment`

| Function | Description |
|----------|-------------|
| `assign_instances(dag, runtimes, deadline, spot_interrupt_buffer=0.2)` | Returns `dict[str, "spot" \| "on-demand"]` mapping each task to its instance type. |

### `spot_scheduler.plugin`

| Class | Description |
|-------|-------------|
| `ProfilingPlugin(profile_path="runtimes.json")` | Dask `SchedulerPlugin` that records task wall-clock runtimes and saves them to JSON. |

Key methods:
- `transition(key, start, finish, ...)` — hooks into task state changes
- `save()` — persist runtimes to disk
- `ProfilingPlugin.load(path)` — static method to load a saved profile

### `spot_scheduler.runner`

| Function | Description |
|----------|-------------|
| `run_dag(client, dag, task_fn, deadline, ...)` | End-to-end execution: profile *or* optimise a DAG on a Dask cluster. |

Parameters:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `client` | `dask.distributed.Client` | — | Connected Dask client |
| `dag` | `dict[str, list[str]]` | — | Task dependency graph |
| `task_fn` | `callable` | — | `task_fn(task_name, *dep_results) → result` |
| `deadline` | `float` | — | Wall-clock deadline in seconds |
| `profile_path` | `str` | `"runtimes.json"` | Path for the runtime profile |
| `spot_interrupt_buffer` | `float` | `0.2` | Safety margin fraction |
| `profiling_mode` | `bool` | `False` | `True` = profiling run, `False` = optimised run |

---

## Running Tests

```bash
pytest -v
```

The test suite covers:
- Topological sort (including cycle detection)
- Earliest / latest time computations
- Slack and critical-path identification
- Spot vs. on-demand assignment under various deadline / buffer scenarios

---

## Dynamic Re-scheduling

The scheduler now supports **dynamic re-scheduling** during execution:

- **Spot interruption detection**: When a spot task fails, it's automatically detected and resubmitted to on-demand
- **Slack recomputation**: As tasks complete (early or late), slack is recomputed for remaining tasks
- **Dynamic reassignment**: Remaining tasks may be reassigned between spot and on-demand based on updated slack

### Testing Dynamic Re-scheduling

```bash
# Simulate spot interruptions
python example.py --deadline 30 --simulate-interruptions B,C

# Test early completion (tasks complete faster)
python example.py --deadline 30 --early-completion 0.5

# Test late completion (tasks complete slower)
python example.py --deadline 30 --early-completion 1.5
```

See `example_configs.md` for more configuration examples.

## What This MVP Skips

These are intentionally left for future iterations:
- **Knapsack / greedy marginal critical-path** heuristics (the slack threshold is sufficient for a baseline)
- **Sensitivity analysis** and cost tracking / reporting
- **Automatic retry** on spot interruption with fallback to on-demand
- **Capacity planning and worker provisioning** — the system assumes workers already exist and only assigns tasks to them; it does not compute how many spot/on-demand workers are needed or launch/scale instances

---

## Algorithm Complexity

The critical path + slack computation is **O(V + E)** on the DAG, so it adds negligible overhead before each run.

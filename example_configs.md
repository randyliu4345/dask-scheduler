# Example Configurations for example.py

This document provides example configurations for testing dynamic re-scheduling with the spot scheduler.

## Basic Usage

### 1. Profiling Run
First, run a profiling pass to record task runtimes:

```bash
python example.py --profile
```

This will:
- Run all tasks on on-demand workers
- Record runtimes to `runtimes.json`
- Create a baseline for subsequent optimised runs

### 2. Optimised Run (Static Assignment)
Run with static spot/on-demand assignment:

```bash
python example.py --deadline 30
```

This will:
- Load runtimes from `runtimes.json`
- Compute critical path and slack
- Assign tasks to spot/on-demand based on slack
- Execute the DAG

## Dynamic Re-scheduling Examples

### 3. Simulate Spot Interruptions
Test dynamic re-scheduling by simulating spot instance interruptions:

```bash
python example.py --deadline 30 --simulate-interruptions B
```

This will:
- Assign tasks based on initial slack analysis
- Simulate an interruption for task B (first attempt fails)
- Detect the interruption and resubmit B to on-demand
- Recompute slack for remaining tasks
- Continue execution

### 4. Multiple Interruptions
Simulate interruptions for multiple tasks:

```bash
python example.py --deadline 30 --simulate-interruptions B,C
```

### 5. Early Completion (More Slack)
Test re-scheduling when tasks complete early:

```bash
python example.py --deadline 30 --early-completion 0.5
```

This will:
- Tasks complete in 50% of their profiled time
- More slack becomes available
- Remaining tasks may be reassigned to spot (if they now have enough slack)

### 6. Late Completion (Less Slack)
Test re-scheduling when tasks complete late:

```bash
python example.py --deadline 30 --early-completion 1.5
```

This will:
- Tasks complete in 150% of their profiled time
- Slack is reduced
- Tasks may be reassigned from spot to on-demand

### 7. Combined: Interruptions + Early Completion
```bash
python example.py --deadline 30 --simulate-interruptions B --early-completion 0.7
```

## Advanced Configuration

### Custom Buffer Factor
Adjust the spot interrupt buffer (default: 0.2 = 20%):

```bash
python example.py --deadline 30 --buffer 0.3
```

Higher buffer = more conservative (fewer spot assignments)

### Custom Profile Path
Use a different profile file:

```bash
python example.py --profile --profile-path my_runtimes.json
python example.py --deadline 30 --profile-path my_runtimes.json
```

### Connect to Remote Cluster
```bash
# On scheduler node:
dask-scheduler

# On worker nodes:
dask-worker tcp://scheduler:8786 --resources "spot=1"
dask-worker tcp://scheduler:8786 --resources "ondemand=1"

# Run example:
python example.py --scheduler tcp://scheduler:8786 --deadline 30 --simulate-interruptions B
```

## Expected Behavior

### With Interruptions
1. Initial assignment: Tasks assigned based on profiled runtimes
2. Task B starts on spot worker
3. Interruption detected: B fails with error
4. Re-scheduling triggered:
   - B is resubmitted to on-demand
   - Slack recomputed for remaining tasks
   - Other tasks may be reassigned
5. Execution continues: B completes on on-demand, remaining tasks execute

### With Early Completion
1. Tasks complete faster than profiled
2. More slack becomes available
3. Remaining tasks may be reassigned to spot (if they gain enough slack)
4. Logs show reassignment messages

### With Late Completion
1. Tasks complete slower than profiled
2. Slack is reduced
3. Tasks may be reassigned from spot to on-demand
4. Critical path may shift

## Monitoring

Watch the logs for:
- `Re-scheduling: X completed, Y interrupted` - re-scheduling triggered
- `Reassigned TASK: spot -> on-demand` - dynamic reassignment
- `Resubmitting interrupted task TASK to on-demand` - interruption handling

Access the Dask dashboard URL shown at startup for real-time monitoring.

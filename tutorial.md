Here's the most minimal version of this system — a **Dask scheduler plugin** that profiles task runtimes on the first run, computes the critical path and slack, then assigns spot vs. on-demand on the second run using a simple greedy heuristic. No dynamic re-scheduling yet.

## Core Data Structures

You need three things upfront: [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/22290763/6e139b16-b1b4-453f-b569-81f6d60fab44/214-Proposal.pdf?AWSAccessKeyId=ASIA2F3EMEYEVV4XTQJE&Signature=a6bW9VSKgBlfaSsgoBLDAkyzgR8%3D&x-amz-security-token=IQoJb3JpZ2luX2VjELD%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJIMEYCIQDJB8OFXbW4AGf6FGxS6R%2FY4HZCarjJxN3sWraP2zN5ogIhAPd%2BQzFEkDql53wKZCct9yj2XGPLyHd7ff8EYi8gXMxFKvMECHkQARoMNjk5NzUzMzA5NzA1Igzu5fosDpE3taJFRxoq0AS6v0VOEgMlw4g8YNUSN8fl%2B39LRml%2BhtqirSRAhQLk4cTx3xbQYgjvLiuHYem6IMOPnuVH7kO8lG7sfoVvcoNbknWwOaRhQDRWGXh4gyGfH9TOscqEpqMobtkvWW5JxmOGQUY%2FWatD1JvHS3sVWhpTlHVawSFwunRpw6AShFzWj4N1O%2BrbGz4LqSB1mI5CcZX120LHiOQ2V2EUr1s%2BqTFi1unfbt0Rf4VHC4Z1LzqA8yKZnOjjwqFkLSz7oyL7eyBHtWjn8qt0CKNxHsGb2UHIYIR%2BYXmPsT8tdilIdBLihg%2BHiZBKdsuBNzB8O8wLKPqFn97kIfalV7zBEp%2FnhBRSO4cCtmZREmQSQWQO9hPOPGdF2RGmuzwbhJ8jLIYeQkBlf4bxo%2F9qvv6B3DqSi%2FJWhyszys2nZq4D6AUj7siiN3msU%2F6aUmuRaBTd71hyNEUTojZ4jL1JX%2BBTMS0%2F4kUjy1twOQHWk58hR5kkQnlLbdqy3wuwvEaTk8p1eIl8EYQvIU1u3%2FuAdeCgWN%2FJ0Zmyd18QuHpSIvz%2BZkP2fBJiHBQJDMml5oCJfd2mfhkMi%2BqY7weeKw4Xhn%2FpNd5chFoRCeQlSwY8t%2BAred2OhD646AraL1M8bM5bWgmapte4Z2X6xktkdfYltjKenI%2B9Gzeap9go0WXZp9I9UR9dtf3PjJ%2FMvJMlGxH18Ea4DV6bG7LH3XqsFmPsu5cOJOHaA%2BuWPXPKzrTSP6enqWGee7lTK7hnA%2Bs3IqNxgrKGjAg4oapL0flfAReuIU3zGwAWeKH8MPGmk80GOpcB3M7nEY9UFYKX1NTVlANpmZeIEUvVip0XAF6p2wKjiB18cfUF8NHaFUcJd2yeUnvBZRvwUkdRzOUH635lv%2BIIIM%2FSvbVoFLapF4X3vUgjyqDxZ6rH9NOiUG0g2riGw4rfaYj4gMMkGa9ayx7WL1nGpa%2B%2Fb4KbG9%2BQ2XO44Ky0NTo6ak6iMkmAZxnRy%2FKCDnH8p%2B83E8%2Bd6Q%3D%3D&Expires=1772411290)
- A **task DAG** (Python `dict` mapping task → list of dependencies)
- **Runtime estimates** per task (loaded from a profile JSON after the first run)
- A **deadline** (in seconds)

## Step 1: Profile Runtimes

On the very first execution, run everything on on-demand and record how long each task takes:

```python
import dask
import time, json

runtime_log = {}

def profiling_task_wrapper(key, fn, *args, **kwargs):
    start = time.time()
    result = fn(*args, **kwargs)
    runtime_log[key] = time.time() - start
    return result

# After the DAG finishes:
with open("runtimes.json", "w") as f:
    json.dump(runtime_log, f)
```

## Step 2: Compute Critical Path & Slack

Given runtimes, find the critical path length and slack per task. The slack on a task is how much you can delay it without pushing past the deadline: [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/22290763/6e139b16-b1b4-453f-b569-81f6d60fab44/214-Proposal.pdf?AWSAccessKeyId=ASIA2F3EMEYEVV4XTQJE&Signature=a6bW9VSKgBlfaSsgoBLDAkyzgR8%3D&x-amz-security-token=IQoJb3JpZ2luX2VjELD%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJIMEYCIQDJB8OFXbW4AGf6FGxS6R%2FY4HZCarjJxN3sWraP2zN5ogIhAPd%2BQzFEkDql53wKZCct9yj2XGPLyHd7ff8EYi8gXMxFKvMECHkQARoMNjk5NzUzMzA5NzA1Igzu5fosDpE3taJFRxoq0AS6v0VOEgMlw4g8YNUSN8fl%2B39LRml%2BhtqirSRAhQLk4cTx3xbQYgjvLiuHYem6IMOPnuVH7kO8lG7sfoVvcoNbknWwOaRhQDRWGXh4gyGfH9TOscqEpqMobtkvWW5JxmOGQUY%2FWatD1JvHS3sVWhpTlHVawSFwunRpw6AShFzWj4N1O%2BrbGz4LqSB1mI5CcZX120LHiOQ2V2EUr1s%2BqTFi1unfbt0Rf4VHC4Z1LzqA8yKZnOjjwqFkLSz7oyL7eyBHtWjn8qt0CKNxHsGb2UHIYIR%2BYXmPsT8tdilIdBLihg%2BHiZBKdsuBNzB8O8wLKPqFn97kIfalV7zBEp%2FnhBRSO4cCtmZREmQSQWQO9hPOPGdF2RGmuzwbhJ8jLIYeQkBlf4bxo%2F9qvv6B3DqSi%2FJWhyszys2nZq4D6AUj7siiN3msU%2F6aUmuRaBTd71hyNEUTojZ4jL1JX%2BBTMS0%2F4kUjy1twOQHWk58hR5kkQnlLbdqy3wuwvEaTk8p1eIl8EYQvIU1u3%2FuAdeCgWN%2FJ0Zmyd18QuHpSIvz%2BZkP2fBJiHBQJDMml5oCJfd2mfhkMi%2BqY7weeKw4Xhn%2FpNd5chFoRCeQlSwY8t%2BAred2OhD646AraL1M8bM5bWgmapte4Z2X6xktkdfYltjKenI%2B9Gzeap9go0WXZp9I9UR9dtf3PjJ%2FMvJMlGxH18Ea4DV6bG7LH3XqsFmPsu5cOJOHaA%2BuWPXPKzrTSP6enqWGee7lTK7hnA%2Bs3IqNxgrKGjAg4oapL0flfAReuIU3zGwAWeKH8MPGmk80GOpcB3M7nEY9UFYKX1NTVlANpmZeIEUvVip0XAF6p2wKjiB18cfUF8NHaFUcJd2yeUnvBZRvwUkdRzOUH635lv%2BIIIM%2FSvbVoFLapF4X3vUgjyqDxZ6rH9NOiUG0g2riGw4rfaYj4gMMkGa9ayx7WL1nGpa%2B%2Fb4KbG9%2BQ2XO44Ky0NTo6ak6iMkmAZxnRy%2FKCDnH8p%2B83E8%2Bd6Q%3D%3D&Expires=1772411290)

```python
import json
from functools import lru_cache

with open("runtimes.json") as f:
    runtimes = json.load(f)

dag = {
    "A": [],
    "B": ["A"],
    "C": ["A"],
    "D": ["B", "C"],
    "E": ["D"],
}

@lru_cache(maxsize=None)
def earliest_finish(task):
    deps = dag[task]
    if not deps:
        return runtimes[task]
    return max(earliest_finish(d) for d in deps) + runtimes[task]

@lru_cache(maxsize=None)
def latest_start(task, deadline):
    successors = [t for t, deps in dag.items() if task in deps]
    if not successors:
        return deadline - runtimes[task]
    return min(latest_start(s, deadline) for s in successors) - runtimes[task]

DEADLINE = 9 * 3600  # e.g. 9 hours

slack = {
    task: latest_start(task, DEADLINE) - (earliest_finish(task) - runtimes[task])
    for task in dag
}
```

## Step 3: Assign Spot vs. On-Demand

The simplest heuristic: tasks with **zero slack are on critical path → on-demand; everything else → spot**: [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/22290763/6e139b16-b1b4-453f-b569-81f6d60fab44/214-Proposal.pdf?AWSAccessKeyId=ASIA2F3EMEYEVV4XTQJE&Signature=a6bW9VSKgBlfaSsgoBLDAkyzgR8%3D&x-amz-security-token=IQoJb3JpZ2luX2VjELD%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJIMEYCIQDJB8OFXbW4AGf6FGxS6R%2FY4HZCarjJxN3sWraP2zN5ogIhAPd%2BQzFEkDql53wKZCct9yj2XGPLyHd7ff8EYi8gXMxFKvMECHkQARoMNjk5NzUzMzA5NzA1Igzu5fosDpE3taJFRxoq0AS6v0VOEgMlw4g8YNUSN8fl%2B39LRml%2BhtqirSRAhQLk4cTx3xbQYgjvLiuHYem6IMOPnuVH7kO8lG7sfoVvcoNbknWwOaRhQDRWGXh4gyGfH9TOscqEpqMobtkvWW5JxmOGQUY%2FWatD1JvHS3sVWhpTlHVawSFwunRpw6AShFzWj4N1O%2BrbGz4LqSB1mI5CcZX120LHiOQ2V2EUr1s%2BqTFi1unfbt0Rf4VHC4Z1LzqA8yKZnOjjwqFkLSz7oyL7eyBHtWjn8qt0CKNxHsGb2UHIYIR%2BYXmPsT8tdilIdBLihg%2BHiZBKdsuBNzB8O8wLKPqFn97kIfalV7zBEp%2FnhBRSO4cCtmZREmQSQWQO9hPOPGdF2RGmuzwbhJ8jLIYeQkBlf4bxo%2F9qvv6B3DqSi%2FJWhyszys2nZq4D6AUj7siiN3msU%2F6aUmuRaBTd71hyNEUTojZ4jL1JX%2BBTMS0%2F4kUjy1twOQHWk58hR5kkQnlLbdqy3wuwvEaTk8p1eIl8EYQvIU1u3%2FuAdeCgWN%2FJ0Zmyd18QuHpSIvz%2BZkP2fBJiHBQJDMml5oCJfd2mfhkMi%2BqY7weeKw4Xhn%2FpNd5chFoRCeQlSwY8t%2BAred2OhD646AraL1M8bM5bWgmapte4Z2X6xktkdfYltjKenI%2B9Gzeap9go0WXZp9I9UR9dtf3PjJ%2FMvJMlGxH18Ea4DV6bG7LH3XqsFmPsu5cOJOHaA%2BuWPXPKzrTSP6enqWGee7lTK7hnA%2Bs3IqNxgrKGjAg4oapL0flfAReuIU3zGwAWeKH8MPGmk80GOpcB3M7nEY9UFYKX1NTVlANpmZeIEUvVip0XAF6p2wKjiB18cfUF8NHaFUcJd2yeUnvBZRvwUkdRzOUH635lv%2BIIIM%2FSvbVoFLapF4X3vUgjyqDxZ6rH9NOiUG0g2riGw4rfaYj4gMMkGa9ayx7WL1nGpa%2B%2Fb4KbG9%2BQ2XO44Ky0NTo6ak6iMkmAZxnRy%2FKCDnH8p%2B83E8%2Bd6Q%3D%3D&Expires=1772411290)

```python
SPOT_INTERRUPT_BUFFER = 0.2  # assume spot adds ~20% risk/delay

assignment = {}
for task, s in slack.items():
    # Give a safety margin: only use spot if slack > buffer * task runtime
    if s > SPOT_INTERRUPT_BUFFER * runtimes[task]:
        assignment[task] = "spot"
    else:
        assignment[task] = "on-demand"

print(assignment)
```

## Step 4: Wire into Dask

Route tasks to the right worker pool using Dask's `resources` feature: [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/22290763/6e139b16-b1b4-453f-b569-81f6d60fab44/214-Proposal.pdf?AWSAccessKeyId=ASIA2F3EMEYEVV4XTQJE&Signature=a6bW9VSKgBlfaSsgoBLDAkyzgR8%3D&x-amz-security-token=IQoJb3JpZ2luX2VjELD%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJIMEYCIQDJB8OFXbW4AGf6FGxS6R%2FY4HZCarjJxN3sWraP2zN5ogIhAPd%2BQzFEkDql53wKZCct9yj2XGPLyHd7ff8EYi8gXMxFKvMECHkQARoMNjk5NzUzMzA5NzA1Igzu5fosDpE3taJFRxoq0AS6v0VOEgMlw4g8YNUSN8fl%2B39LRml%2BhtqirSRAhQLk4cTx3xbQYgjvLiuHYem6IMOPnuVH7kO8lG7sfoVvcoNbknWwOaRhQDRWGXh4gyGfH9TOscqEpqMobtkvWW5JxmOGQUY%2FWatD1JvHS3sVWhpTlHVawSFwunRpw6AShFzWj4N1O%2BrbGz4LqSB1mI5CcZX120LHiOQ2V2EUr1s%2BqTFi1unfbt0Rf4VHC4Z1LzqA8yKZnOjjwqFkLSz7oyL7eyBHtWjn8qt0CKNxHsGb2UHIYIR%2BYXmPsT8tdilIdBLihg%2BHiZBKdsuBNzB8O8wLKPqFn97kIfalV7zBEp%2FnhBRSO4cCtmZREmQSQWQO9hPOPGdF2RGmuzwbhJ8jLIYeQkBlf4bxo%2F9qvv6B3DqSi%2FJWhyszys2nZq4D6AUj7siiN3msU%2F6aUmuRaBTd71hyNEUTojZ4jL1JX%2BBTMS0%2F4kUjy1twOQHWk58hR5kkQnlLbdqy3wuwvEaTk8p1eIl8EYQvIU1u3%2FuAdeCgWN%2FJ0Zmyd18QuHpSIvz%2BZkP2fBJiHBQJDMml5oCJfd2mfhkMi%2BqY7weeKw4Xhn%2FpNd5chFoRCeQlSwY8t%2BAred2OhD646AraL1M8bM5bWgmapte4Z2X6xktkdfYltjKenI%2B9Gzeap9go0WXZp9I9UR9dtf3PjJ%2FMvJMlGxH18Ea4DV6bG7LH3XqsFmPsu5cOJOHaA%2BuWPXPKzrTSP6enqWGee7lTK7hnA%2Bs3IqNxgrKGjAg4oapL0flfAReuIU3zGwAWeKH8MPGmk80GOpcB3M7nEY9UFYKX1NTVlANpmZeIEUvVip0XAF6p2wKjiB18cfUF8NHaFUcJd2yeUnvBZRvwUkdRzOUH635lv%2BIIIM%2FSvbVoFLapF4X3vUgjyqDxZ6rH9NOiUG0g2riGw4rfaYj4gMMkGa9ayx7WL1nGpa%2B%2Fb4KbG9%2BQ2XO44Ky0NTo6ak6iMkmAZxnRy%2FKCDnH8p%2B83E8%2Bd6Q%3D%3D&Expires=1772411290)

```python
import dask.distributed as dd

# Assume you've launched workers tagged with resources:
# dask-worker ... --resources "spot=1"   (your spot fleet)
# dask-worker ... --resources "ondemand=1" (your on-demand fleet)

client = dd.Client("scheduler-address:8786")

futures = {}
for task in topo_sort(dag):  # implement topo_sort via Kahn's algorithm
    deps_done = [futures[d] for d in dag[task]]
    resource = {"spot": 1} if assignment[task] == "spot" else {"ondemand": 1}
    futures[task] = client.submit(
        your_task_fn, task, *deps_done,
        resources=resource,
        key=task
    )

dd.wait(list(futures.values()))
```

## What This MVP Skips

To keep it minimal, leave these for later: [ppl-ai-file-upload.s3.amazonaws](https://ppl-ai-file-upload.s3.amazonaws.com/web/direct-files/attachments/22290763/6e139b16-b1b4-453f-b569-81f6d60fab44/214-Proposal.pdf?AWSAccessKeyId=ASIA2F3EMEYEVV4XTQJE&Signature=a6bW9VSKgBlfaSsgoBLDAkyzgR8%3D&x-amz-security-token=IQoJb3JpZ2luX2VjELD%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaCXVzLWVhc3QtMSJIMEYCIQDJB8OFXbW4AGf6FGxS6R%2FY4HZCarjJxN3sWraP2zN5ogIhAPd%2BQzFEkDql53wKZCct9yj2XGPLyHd7ff8EYi8gXMxFKvMECHkQARoMNjk5NzUzMzA5NzA1Igzu5fosDpE3taJFRxoq0AS6v0VOEgMlw4g8YNUSN8fl%2B39LRml%2BhtqirSRAhQLk4cTx3xbQYgjvLiuHYem6IMOPnuVH7kO8lG7sfoVvcoNbknWwOaRhQDRWGXh4gyGfH9TOscqEpqMobtkvWW5JxmOGQUY%2FWatD1JvHS3sVWhpTlHVawSFwunRpw6AShFzWj4N1O%2BrbGz4LqSB1mI5CcZX120LHiOQ2V2EUr1s%2BqTFi1unfbt0Rf4VHC4Z1LzqA8yKZnOjjwqFkLSz7oyL7eyBHtWjn8qt0CKNxHsGb2UHIYIR%2BYXmPsT8tdilIdBLihg%2BHiZBKdsuBNzB8O8wLKPqFn97kIfalV7zBEp%2FnhBRSO4cCtmZREmQSQWQO9hPOPGdF2RGmuzwbhJ8jLIYeQkBlf4bxo%2F9qvv6B3DqSi%2FJWhyszys2nZq4D6AUj7siiN3msU%2F6aUmuRaBTd71hyNEUTojZ4jL1JX%2BBTMS0%2F4kUjy1twOQHWk58hR5kkQnlLbdqy3wuwvEaTk8p1eIl8EYQvIU1u3%2FuAdeCgWN%2FJ0Zmyd18QuHpSIvz%2BZkP2fBJiHBQJDMml5oCJfd2mfhkMi%2BqY7weeKw4Xhn%2FpNd5chFoRCeQlSwY8t%2BAred2OhD646AraL1M8bM5bWgmapte4Z2X6xktkdfYltjKenI%2B9Gzeap9go0WXZp9I9UR9dtf3PjJ%2FMvJMlGxH18Ea4DV6bG7LH3XqsFmPsu5cOJOHaA%2BuWPXPKzrTSP6enqWGee7lTK7hnA%2Bs3IqNxgrKGjAg4oapL0flfAReuIU3zGwAWeKH8MPGmk80GOpcB3M7nEY9UFYKX1NTVlANpmZeIEUvVip0XAF6p2wKjiB18cfUF8NHaFUcJd2yeUnvBZRvwUkdRzOUH635lv%2BIIIM%2FSvbVoFLapF4X3vUgjyqDxZ6rH9NOiUG0g2riGw4rfaYj4gMMkGa9ayx7WL1nGpa%2B%2Fb4KbG9%2BQ2XO44Ky0NTo6ak6iMkmAZxnRy%2FKCDnH8p%2B83E8%2Bd6Q%3D%3D&Expires=1772411290)
- **Dynamic re-scheduling** mid-run based on whether tasks finish early/late
- **Knapsack or greedy marginal critical path** heuristics (the simple slack threshold above is sufficient for a baseline)
- **Sensitivity analysis** and cost tracking

The critical path + slack computation is \(O(V + E)\) on the DAG, so it adds negligible overhead before each run.
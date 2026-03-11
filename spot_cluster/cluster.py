"""
Cluster Setup: Simulated Spot vs Real AWS/GCP
=============================================
Switch between environments with a single env var or config flag.

  LOCAL_SIM=1 python my_pipeline.py        # simulated spot, runs on laptop
  AWS_PROFILE=prod python my_pipeline.py   # real AWS spot instances
"""

from __future__ import annotations
import os
import asyncio
import random
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Literal, Optional

from distributed import Client, LocalCluster, Worker

# ──────────────────────────────────────────────
# 0. SHARED CONFIG
# ──────────────────────────────────────────────

import time

class WorkerCostTracker:
    def __init__(self, config: ClusterConfig):
        self.config = config
        self._start_times = {}
        self._total_cost = 0.0

    def worker_started(self, address: str, pool: str):
        self._start_times[address] = (time.time(), pool)

    def worker_stopped(self, address: str):
        if address not in self._start_times:
            return

        start, pool = self._start_times.pop(address)
        runtime = time.time() - start

        price = (
            self.config.spot_price_per_hour
            if pool == "spot"
            else self.config.ondemand_price_per_hour
        )

        cost = runtime / 3600 * price
        self._total_cost += cost

        print(
            f"[cost] worker {address} ({pool}) ran {runtime:.1f}s → ${cost:.4f}"
        )

    def total_cost(self):
        return self._total_cost

@dataclass
class WorkerPoolConfig:
    n_workers:       int   = 4
    threads_per:     int   = 2
    memory_per:      str   = "4GB"

@dataclass
class ClusterConfig:
    mode: Literal["local_sim", "aws", "gcp"] = "local_sim"

    # --- simulation knobs ---
    spot_interruption_prob: float         = 0.10   # per-task probability
    interruption_interval_s: float        = 30.0   # check every N seconds

    # --- shared pool sizing ---
    spot:     WorkerPoolConfig = field(default_factory=lambda: WorkerPoolConfig(n_workers=1, threads_per=1))
    ondemand: WorkerPoolConfig = field(default_factory=lambda: WorkerPoolConfig(n_workers=1, threads_per=1))

    # --- AWS-specific ---
    aws_region:           str = "us-east-1"
    aws_spot_ami:         str = ""
    aws_instance_spot:    str = "m5.xlarge"
    aws_instance_ondemand:str = "m5.xlarge"
    aws_subnet_id:        str = ""
    aws_security_group:   str = ""
    aws_s3_log_bucket:    str = ""

    # --- GCP-specific ---
    gcp_project:          str = ""
    gcp_zone:             str = "us-central1-a"
    gcp_spot_machine:     str = "n2-standard-4"
    gcp_ondemand_machine: str = "n2-standard-4"

    spot_price_per_hour: float     = 0.04
    ondemand_price_per_hour: float = 0.40

    @staticmethod
    def from_env() -> "ClusterConfig":
        """Auto-detect mode from environment variables."""
        if os.getenv("LOCAL_SIM"):
            return ClusterConfig(mode="local_sim")
        if os.getenv("AWS_PROFILE") or os.getenv("AWS_ACCESS_KEY_ID"):
            return ClusterConfig(
                mode="aws",
                aws_region=os.getenv("AWS_REGION", "us-east-1"),
                aws_spot_ami=os.getenv("AWS_AMI", ""),
                aws_subnet_id=os.getenv("AWS_SUBNET_ID", ""),
                aws_security_group=os.getenv("AWS_SG", ""),
                aws_s3_log_bucket=os.getenv("AWS_S3_LOG_BUCKET", ""),
            )
        if os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT"):
            return ClusterConfig(
                mode="gcp",
                gcp_project=os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT"),
                gcp_zone=os.getenv("GCP_ZONE", "us-central1-a"),
            )
        return ClusterConfig(mode="local_sim")   # safe default


# ──────────────────────────────────────────────
# 1. ABSTRACT BASE — both providers share this interface
# ──────────────────────────────────────────────

class BaseCluster(ABC):
    """Common interface regardless of backend."""

    @abstractmethod
    def start(self) -> "ClusterAddresses": ...

    @abstractmethod
    def stop(self): ...

    # @abstractmethod
    # def scale_spot(self, n: int): ...

    # @abstractmethod
    # def scale_ondemand(self, n: int): ...

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, *_):
        self.stop()

@dataclass
class ClusterAddresses:
    scheduler:        str
    spot_workers:     list[str]
    ondemand_workers: list[str]
    dashboard:        str


# ──────────────────────────────────────────────
# 2. LOCAL SIMULATION CLUSTER
# ──────────────────────────────────────────────

class SimulatedSpotCluster(BaseCluster):
    """
    Runs entirely on localhost using dask.distributed LocalCluster.
    A background thread randomly kills spot workers to mimic preemptions.

    No cloud credentials needed — perfect for development and CI.
    """

    def __init__(self, config: ClusterConfig):
        self.config = config
        self._cluster   = None
        self._client    = None
        self._killer: Optional[threading.Thread]    = None

    def start(self) -> ClusterAddresses:
        self._cluster = LocalCluster(n_workers=0, dashboard_address=":8788")
        self._cost = WorkerCostTracker(self.config)

        async def setup_workers(cluster, spot_config, ondemand_config):
            addr = cluster.scheduler_address

            for _ in range(spot_config.n_workers):
                w = await Worker(addr, resources={"spot": 1}, nthreads=spot_config.threads_per)
                self._cost.worker_started(w.address, "spot")

            for _ in range(ondemand_config.n_workers):
                w = await Worker(addr, resources={"ondemand": 1}, nthreads=ondemand_config.threads_per)
                self._cost.worker_started(w.address, "ondemand")

        self._cluster.sync(setup_workers, self._cluster, spot_config=self.config.spot, ondemand_config=self.config.ondemand)
        self._start_interruption_thread()

       

        return ClusterAddresses(
            scheduler = self._cluster.scheduler_address,  # <-- single address, all workers visible
            dashboard = "http://localhost:8788",
            spot_workers=[],
            ondemand_workers=[]
        )
    
    def _start_interruption_thread(self):
        def _killer_loop():
            assert self._cluster is not None
            while not self._stop_event.is_set():
                self._stop_event.wait(self.config.interruption_interval_s)
                if self._stop_event.is_set():
                    break

                scheduler = self._cluster.scheduler

                # only retire workers that have the spot resource tag
                spot_workers = [
                    w.address for w in scheduler.workers.values()
                    if "spot" in (w.resources or {})
                ]

                async def _preempt(addr):
                    # first kill the worker process itself
                    if addr in scheduler.workers:
                        self._cost.worker_stopped(addr)
                        ws = scheduler.workers[addr]
                        # send SIGKILL to the worker comm
                        await scheduler.stream_comms[addr].close()
                    # then remove it from scheduler state
                    await scheduler.remove_worker(address=addr, stimulus_id="simulated-preemption")

                for addr in spot_workers:
                    if random.random() < self.config.spot_interruption_prob:
                        print(f"[sim] 🔴 Preempting spot worker {addr}")
                        # asyncio.run_coroutine_threadsafe(
                        #     scheduler.retire_workers(
                        #         workers      = [addr],
                        #         close_workers = True,
                        #     ),
                        #     self._cluster.loop.asyncio_loop,
                        # )
                        asyncio.run_coroutine_threadsafe(_preempt(addr), self._cluster.loop.asyncio_loop)
                        # respawn a replacement spot worker after delay
                        threading.Timer(5.0, self._respawn_spot_worker).start()

        self._stop_event = threading.Event()
        self._killer = threading.Thread(target=_killer_loop, daemon=True).start()

    def _respawn_spot_worker(self):
        assert self._cluster is not None
        asyncio_loop = self._cluster.loop.asyncio_loop

        # after job ends, stop respawning
        if asyncio_loop.is_closed():
            return

        async def _start():
            assert self._cluster is not None
            w = await Worker(
                self._cluster.scheduler_address,
                nthreads     = self.config.spot.threads_per,
                memory_limit = self.config.spot.memory_per,
                resources    = {"spot": 1},
            )

            self._cost.worker_started(w.address, "spot")

        asyncio.run_coroutine_threadsafe(_start(), asyncio_loop)
        print("[sim] 🟢 Replacement spot worker started")
    # def scale_spot(self, n: int):
    #     if self._cluster:
    #         self._cluster.scale(n)

    # def scale_ondemand(self, n: int):
    #     # self._od_cluster.scale(n)
    #     pass

    def stop(self):
        if self._stop_event:
            self._stop_event.set()
            
        if self._cluster:
            for w in list(self._cluster.scheduler.workers):
                self._cost.worker_stopped(w)
            print(f"\nEstimated cluster cost: ${self._cost.total_cost():.4f}")
            self._cluster.close()
        # if self._spot_cluster:
        #     self._spot_cluster.close()
        # if self._od_cluster:
        #     self._od_cluster.close()


# ──────────────────────────────────────────────
# 3. AWS CLUSTER  (EC2 Spot + On-Demand)
# ──────────────────────────────────────────────

class AWSCluster(BaseCluster):
    """
    Uses dask-cloudprovider EC2Cluster for each pool.
    Spot pool uses SpotInstanceMarketOptions; on-demand pool is standard EC2.

    Requires: pip install dask-cloudprovider[aws]
    """

    def __init__(self, config: ClusterConfig):
        self.config = config
        self._spot_cluster = None
        self._od_cluster   = None

    def start(self) -> ClusterAddresses:
        from dask_cloudprovider.aws import EC2Cluster

        common = dict(
            region          = self.config.aws_region,
            ami             = self.config.aws_spot_ami,
            subnet_id       = self.config.aws_subnet_id,
            security_groups = [self.config.aws_security_group],
            # workers auto-install dask on startup:
            bootstrap       = True,
            auto_shutdown   = True,
        )

        # ── spot pool ──
        self._spot_cluster = EC2Cluster(
            **common,
            instance_type   = self.config.aws_instance_spot,
            n_workers       = self.config.spot.n_workers,
            worker_options  = {"resources": {"spot": 1}},
            # request spot capacity:
            instance_market_options = {
                "MarketType": "spot",
                "SpotOptions": {
                    "SpotInstanceType": "one-time",
                    "InstanceInterruptionBehavior": "terminate",
                },
            },
        )

        # ── on-demand pool ──
        self._od_cluster = EC2Cluster(
            **common,
            instance_type   = self.config.aws_instance_ondemand,
            n_workers       = self.config.ondemand.n_workers,
            worker_options  = {"resources": {"ondemand": 1}},
            # no instance_market_options → standard on-demand
        )

        # Attach ITN (instance termination notice) handler
        # AWS gives 2-minute warning via instance metadata
        self._attach_itn_handler()

        return ClusterAddresses(
            scheduler        = self._spot_cluster.scheduler_address,
            spot_workers     = [],   # populated dynamically by dask-cloudprovider
            ondemand_workers = [],
            dashboard        = self._spot_cluster.dashboard_link,
        )

    def _attach_itn_handler(self):
        """
        Poll EC2 instance metadata for spot termination notices.
        Gracefully drains the worker before the 2-minute window expires.
        This runs inside each spot worker process.

        In practice, inject this as a Worker plugin or preload script.
        """
        # See: dask_deadline/worker_plugins/itn_watcher.py  (implement separately)
        pass

    # def scale_spot(self, n: int):
    #     self._spot_cluster.scale(n)

    # def scale_ondemand(self, n: int):
    #     self._od_cluster.scale(n)

    def stop(self):
        if self._spot_cluster: self._spot_cluster.close()
        if self._od_cluster:   self._od_cluster.close()


# ──────────────────────────────────────────────
# 4. GCP CLUSTER  (Spot VMs + On-Demand)
# ──────────────────────────────────────────────

class GCPCluster(BaseCluster):
    """
    Uses dask-cloudprovider GCPCluster for each pool.
    Spot pool sets on_host_maintenance="TERMINATE" + preemptible=True.

    Requires: pip install dask-cloudprovider[gcp]
    """

    def __init__(self, config: ClusterConfig):
        self.config = config

    def start(self) -> ClusterAddresses:
        import asyncio
        from dask_cloudprovider.gcp import GCPCluster
        from dask_cloudprovider.gcp.instances import GCPWorker

        common = dict(
            projectid         = self.config.gcp_project,
            zone              = self.config.gcp_zone,
            bootstrap         = True,
            auto_shutdown     = True,
        )

        # spot cluster owns the scheduler
        self._cluster = GCPCluster(
            **common,
            scheduler_machine_type = "n1-standard-2",
            worker_machine_type    = self.config.gcp_spot_machine,
            n_workers              = self.config.spot.n_workers,
            worker_options         = {"resources": {"spot": 1}},
            on_host_maintenance    = "TERMINATE",
            preemptible            = True,
        )

        # on-demand workers join the same scheduler
        self._od_workers = [
            GCPWorker(
                scheduler      = self._cluster.scheduler_address,
                cluster        = self._cluster,
                config         = self._cluster.config,
                machine_type   = self.config.gcp_ondemand_machine,
                worker_options = {"resources": {"ondemand": 1}},
                preemptible    = False,
            )
            for _ in range(self.config.ondemand.n_workers)
        ]

        asyncio.get_event_loop().run_until_complete(
            asyncio.gather(*[w.start() for w in self._od_workers])
        )

        return ClusterAddresses(
            scheduler = self._cluster.scheduler_address,
            dashboard = self._cluster.dashboard_link,
            spot_workers=[],
            ondemand_workers=[]
        )

    def stop(self):
        import asyncio
        if self._od_workers:
            asyncio.get_event_loop().run_until_complete(
                asyncio.gather(*[w.close() for w in self._od_workers])
            )
        if self._cluster:
            self._cluster.close()

    # def scale_spot(self, n: int): self._spot_cluster.scale(n)
    # def scale_ondemand(self, n: int): self._od_cluster.scale(n)

# ──────────────────────────────────────────────
# 5. FACTORY  — the only thing callers need to import
# ──────────────────────────────────────────────

def make_cluster(config: ClusterConfig | None = None) -> BaseCluster:
    """
    Returns the right cluster implementation for the current environment.

    Usage:
        cluster = make_cluster()                          # auto-detect from env
        cluster = make_cluster(ClusterConfig(mode="aws")) # explicit
    """
    cfg = config or ClusterConfig.from_env()
    match cfg.mode:
        case "local_sim": return SimulatedSpotCluster(cfg)
        case "aws":       return AWSCluster(cfg)
        case "gcp":       return GCPCluster(cfg)
        case _:           raise ValueError(f"Unknown mode: {cfg.mode}")


# ──────────────────────────────────────────────
# 6. USAGE EXAMPLES
# ──────────────────────────────────────────────

if __name__ == "__main__":
    import dask.array as da

    # --- auto-detect from environment ---
    cluster = make_cluster()

    with cluster:
        addrs = cluster.start()

        client = Client(
            scheduler_address = addrs.scheduler,
            worker_pools = {
                "spot":     {"cost": 0.04},
                "ondemand": {"cost": 0.40},
            },
        )

        # x = da.random.random((10_000, 10_000), chunks=(1_000, 1_000))
        # result = DeadlineGraph(x.mean(), deadline=120).compute()
        # print(result)
        # print(client.deadline_report())

    # --- explicit local sim with aggressive interruptions (stress test) ---
    stress_cfg = ClusterConfig(
        mode                    = "local_sim",
        spot_interruption_prob  = 0.50,   # 50% chance — brutal
        interruption_interval_s = 10.0,
    )
    with make_cluster(stress_cfg) as cluster:
        addrs = cluster.start()
        # ... run your pipeline

    # --- explicit AWS ---
    aws_cfg = ClusterConfig(
        mode                  = "aws",
        aws_region            = "us-west-2",
        aws_instance_spot     = "c5.2xlarge",
        aws_instance_ondemand = "c5.2xlarge",
        spot = WorkerPoolConfig(n_workers=10),
        ondemand = WorkerPoolConfig(n_workers=2),
    )
    gcp_cfg = ClusterConfig(
        mode                  = "gcp",
        gcp_project           = "my-gcp-project",
        gcp_zone              = "us-central1-a",
        gcp_spot_machine      = "n2-standard-4",
        gcp_ondemand_machine  = "n2-standard-8",   # beefier on-demand for critical tasks
        spot     = WorkerPoolConfig(n_workers=10, threads_per=4, memory_per="16GB"),
        ondemand = WorkerPoolConfig(n_workers=2,  threads_per=8, memory_per="32GB"),
    )
    with make_cluster(aws_cfg) as cluster:
        addrs = cluster.start()
        # ... run your pipeline
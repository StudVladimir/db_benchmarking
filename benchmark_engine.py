import argparse
import csv
import datetime as dt
import json
import math
import os
import statistics
import subprocess
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


ALL_SERVICES = ["mysql", "postgres", "redis", "sqlite"]


@dataclass
class QueryDef:
    name: str
    sql: str
    params_factory: Callable[[int, dict[str, Any]], tuple[Any, ...]]


def run_cmd(args: list[str], cwd: str | None = None, check: bool = True) -> subprocess.CompletedProcess:
    proc = subprocess.run(args, cwd=cwd, text=True, capture_output=True)
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"Command failed ({proc.returncode}): {' '.join(args)}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
    return proc


def parse_percent(value: str) -> float:
    try:
        return float(value.strip().replace("%", ""))
    except Exception:
        return float("nan")


def parse_size(value: str) -> float:
    value = value.strip()
    if not value:
        return float("nan")
    value = value.replace("iB", "IB").replace("Ki", "K").replace("Mi", "M").replace("Gi", "G")
    units = {
        "B": 1,
        "KB": 1000,
        "MB": 1000**2,
        "GB": 1000**3,
        "TB": 1000**4,
        "KIB": 1024,
        "MIB": 1024**2,
        "GIB": 1024**3,
        "TIB": 1024**4,
        "K": 1000,
        "M": 1000**2,
        "G": 1000**3,
        "T": 1000**4,
    }

    num = []
    unit = []
    for ch in value:
        if ch.isdigit() or ch in ".-":
            num.append(ch)
        elif ch.isalpha():
            unit.append(ch.upper())
    try:
        number = float("".join(num))
    except Exception:
        return float("nan")
    unit_key = "".join(unit) or "B"
    mult = units.get(unit_key, 1)
    return number * mult


def parse_io_pair(value: str) -> tuple[float, float]:
    parts = value.split("/")
    if len(parts) != 2:
        return float("nan"), float("nan")
    return parse_size(parts[0]), parse_size(parts[1])


def percentile(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    if p <= 0:
        return min(values)
    if p >= 100:
        return max(values)
    ordered = sorted(values)
    idx = (len(ordered) - 1) * (p / 100.0)
    lo = math.floor(idx)
    hi = math.ceil(idx)
    if lo == hi:
        return ordered[lo]
    frac = idx - lo
    return ordered[lo] * (1 - frac) + ordered[hi] * frac


class DockerStatsSampler:
    def __init__(self, container_name: str, phase: str, interval_sec: float = 1.0):
        self.container_name = container_name
        self.phase = phase
        self.interval_sec = interval_sec
        self.samples: list[dict[str, Any]] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stop.is_set():
            ts = dt.datetime.now(dt.timezone.utc).isoformat()
            try:
                proc = run_cmd(
                    [
                        "docker",
                        "stats",
                        "--no-stream",
                        "--format",
                        "{{json .}}",
                        self.container_name,
                    ],
                    check=True,
                )
                line = proc.stdout.strip().splitlines()[0] if proc.stdout.strip() else "{}"
                payload = json.loads(line)
                mem_usage = payload.get("MemUsage", "")
                net_io = payload.get("NetIO", "")
                block_io = payload.get("BlockIO", "")
                mem_used, mem_limit = parse_io_pair(mem_usage)
                net_in, net_out = parse_io_pair(net_io)
                block_in, block_out = parse_io_pair(block_io)
                self.samples.append(
                    {
                        "timestamp_utc": ts,
                        "phase": self.phase,
                        "cpu_percent": parse_percent(payload.get("CPUPerc", "")),
                        "mem_percent": parse_percent(payload.get("MemPerc", "")),
                        "mem_used_bytes": mem_used,
                        "mem_limit_bytes": mem_limit,
                        "net_in_bytes": net_in,
                        "net_out_bytes": net_out,
                        "block_in_bytes": block_in,
                        "block_out_bytes": block_out,
                        "cpu_raw": payload.get("CPUPerc", ""),
                        "mem_raw": payload.get("MemUsage", ""),
                        "net_raw": payload.get("NetIO", ""),
                        "block_raw": payload.get("BlockIO", ""),
                    }
                )
            except Exception:
                self.samples.append(
                    {
                        "timestamp_utc": ts,
                        "phase": self.phase,
                        "cpu_percent": float("nan"),
                        "mem_percent": float("nan"),
                        "mem_used_bytes": float("nan"),
                        "mem_limit_bytes": float("nan"),
                        "net_in_bytes": float("nan"),
                        "net_out_bytes": float("nan"),
                        "block_in_bytes": float("nan"),
                        "block_out_bytes": float("nan"),
                        "cpu_raw": "",
                        "mem_raw": "",
                        "net_raw": "",
                        "block_raw": "",
                    }
                )
            self._stop.wait(self.interval_sec)


def summarize_docker_stats(samples: list[dict[str, Any]]) -> dict[str, Any]:
    if not samples:
        return {
            "avg_cpu_percent": float("nan"),
            "max_cpu_percent": float("nan"),
            "avg_mem_percent": float("nan"),
            "max_mem_percent": float("nan"),
            "mem_limit_bytes": float("nan"),
            "net_in_delta_bytes": float("nan"),
            "net_out_delta_bytes": float("nan"),
            "block_in_delta_bytes": float("nan"),
            "block_out_delta_bytes": float("nan"),
        }

    cpu_values = [x["cpu_percent"] for x in samples if not math.isnan(x["cpu_percent"])]
    mem_values = [x["mem_percent"] for x in samples if not math.isnan(x["mem_percent"])]

    first = samples[0]
    last = samples[-1]

    return {
        "avg_cpu_percent": statistics.fmean(cpu_values) if cpu_values else float("nan"),
        "max_cpu_percent": max(cpu_values) if cpu_values else float("nan"),
        "avg_mem_percent": statistics.fmean(mem_values) if mem_values else float("nan"),
        "max_mem_percent": max(mem_values) if mem_values else float("nan"),
        "mem_limit_bytes": last.get("mem_limit_bytes", float("nan")),
        "net_in_delta_bytes": last.get("net_in_bytes", float("nan")) - first.get("net_in_bytes", float("nan")),
        "net_out_delta_bytes": last.get("net_out_bytes", float("nan")) - first.get("net_out_bytes", float("nan")),
        "block_in_delta_bytes": last.get("block_in_bytes", float("nan")) - first.get("block_in_bytes", float("nan")),
        "block_out_delta_bytes": last.get("block_out_bytes", float("nan")) - first.get("block_out_bytes", float("nan")),
    }


def ensure_single_active_service(
    target_service: str,
    target_container: str,
    memory_limit: str,
    cpus: float,
    recreate_container: bool,
    wait_ready: Callable[[], None] | None = None,
) -> None:
    other_services = [s for s in ALL_SERVICES if s != target_service]
    if other_services:
        run_cmd(["docker", "compose", "stop", *other_services], check=False)

    if recreate_container:
        run_cmd(["docker", "compose", "rm", "-sf", target_service], check=False)

    run_cmd(["docker", "compose", "up", "-d", target_service], check=True)

    run_cmd(
        [
            "docker",
            "update",
            "--cpus",
            str(cpus),
            "--memory",
            memory_limit,
            "--memory-swap",
            memory_limit,
            target_container,
        ],
        check=True,
    )

    if wait_ready is not None:
        wait_ready()


def run_import_script(csv_folder: str, flags: list[str], reset_data: bool) -> None:
    cmd = [
        "powershell",
        "-ExecutionPolicy",
        "Bypass",
        "-File",
        ".\\import_csv_to_dbs.ps1",
        "-CsvFolder",
        csv_folder,
    ]
    if not reset_data:
        cmd.append("-ResetData:$false")
    cmd.extend(flags)
    run_cmd(cmd, check=True)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


class BenchmarkRunner:
    def __init__(
        self,
        db_name: str,
        service_name: str,
        container_name: str,
        connect_factory: Callable[[], Any],
        execute_query: Callable[[Any, str, tuple[Any, ...]], list[Any]],
        close_connection: Callable[[Any], None],
        query_defs: list[QueryDef],
        import_flags: list[str],
        load_seed_data: Callable[[Any], dict[str, Any]],
        wait_ready: Callable[[], None] | None = None,
    ):
        self.db_name = db_name
        self.service_name = service_name
        self.container_name = container_name
        self.connect_factory = connect_factory
        self.execute_query = execute_query
        self.close_connection = close_connection
        self.query_defs = query_defs
        self.import_flags = import_flags
        self.load_seed_data = load_seed_data
        self.wait_ready = wait_ready

    def run(self, args: argparse.Namespace) -> None:
        stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_dir = Path(args.results_dir) / self.db_name / f"{stamp}_mem-{args.memory_limit}_u-{args.concurrency}"
        out_dir.mkdir(parents=True, exist_ok=True)

        latencies: list[dict[str, Any]] = []
        phase_summary: list[dict[str, Any]] = []
        docker_stats_all: list[dict[str, Any]] = []

        ensure_single_active_service(
            target_service=self.service_name,
            target_container=self.container_name,
            memory_limit=args.memory_limit,
            cpus=args.cpus,
            recreate_container=args.recreate_container,
            wait_ready=self.wait_ready,
        )

        if not args.skip_load_data:
            sampler = DockerStatsSampler(self.container_name, "load_data", args.stats_interval_sec)
            t0 = time.perf_counter()
            sampler.start()
            run_import_script(args.csv_folder, self.import_flags, args.reset_data)
            sampler.stop()
            elapsed = time.perf_counter() - t0
            docker_stats_all.extend(sampler.samples)
            sys_metrics = summarize_docker_stats(sampler.samples)
            phase_summary.append(
                {
                    "db": self.db_name,
                    "phase": "load_data",
                    "duration_sec": elapsed,
                    "total_requests": 0,
                    "success_requests": 0,
                    "error_requests": 0,
                    "ops_per_sec": 0.0,
                    "p50_ms": float("nan"),
                    "p95_ms": float("nan"),
                    "p99_ms": float("nan"),
                    **sys_metrics,
                }
            )

        conn = self.connect_factory()
        try:
            seed = self.load_seed_data(conn)
        finally:
            self.close_connection(conn)

        warmup_summary, warmup_lat, warmup_stats = self._run_query_phase(
            phase_name="warmup",
            total_requests=args.warmup_requests,
            concurrency=args.concurrency,
            seed=seed,
            stats_interval=args.stats_interval_sec,
        )
        run_summary, run_lat, run_stats = self._run_query_phase(
            phase_name="benchmark",
            total_requests=args.benchmark_requests,
            concurrency=args.concurrency,
            seed=seed,
            stats_interval=args.stats_interval_sec,
        )

        phase_summary.extend([warmup_summary, run_summary])
        latencies.extend(warmup_lat)
        latencies.extend(run_lat)
        docker_stats_all.extend(warmup_stats)
        docker_stats_all.extend(run_stats)

        write_csv(out_dir / "latencies.csv", latencies)
        write_csv(out_dir / "phase_summary.csv", phase_summary)
        write_csv(out_dir / "docker_stats_samples.csv", docker_stats_all)

        print(f"Completed. Results saved to: {out_dir}")

    def _run_query_phase(
        self,
        phase_name: str,
        total_requests: int,
        concurrency: int,
        seed: dict[str, Any],
        stats_interval: float,
    ) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
        lat_rows: list[dict[str, Any]] = []
        sampler = DockerStatsSampler(self.container_name, phase_name, stats_interval)
        conn_lock = threading.Lock()
        conns: list[Any] = []
        thread_local = threading.local()

        query_count = len(self.query_defs)

        def get_conn() -> Any:
            conn = getattr(thread_local, "conn", None)
            if conn is None:
                conn = self.connect_factory()
                thread_local.conn = conn
                with conn_lock:
                    conns.append(conn)
            return conn

        def execute_one(i: int) -> None:
            qdef = self.query_defs[i % query_count]
            seq = i // query_count
            params = qdef.params_factory(seq, seed)
            started = time.perf_counter()
            ok = True
            err = ""
            try:
                conn = get_conn()
                _ = self.execute_query(conn, qdef.sql, params)
            except Exception as ex:
                ok = False
                err = str(ex)
            elapsed_ms = (time.perf_counter() - started) * 1000.0
            lat_rows.append(
                {
                    "phase": phase_name,
                    "request_index": i,
                    "query_name": qdef.name,
                    "latency_ms": elapsed_ms,
                    "ok": ok,
                    "error": err,
                }
            )

        t0 = time.perf_counter()
        sampler.start()
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            list(pool.map(execute_one, range(total_requests)))
        sampler.stop()
        elapsed = time.perf_counter() - t0

        for conn in conns:
            try:
                self.close_connection(conn)
            except Exception:
                pass

        success_lat = [x["latency_ms"] for x in lat_rows if x["ok"]]
        success_count = len(success_lat)
        error_count = len(lat_rows) - success_count

        sys_metrics = summarize_docker_stats(sampler.samples)

        summary = {
            "db": self.db_name,
            "phase": phase_name,
            "duration_sec": elapsed,
            "total_requests": len(lat_rows),
            "success_requests": success_count,
            "error_requests": error_count,
            "ops_per_sec": (len(lat_rows) / elapsed) if elapsed > 0 else float("nan"),
            "p50_ms": percentile(success_lat, 50),
            "p95_ms": percentile(success_lat, 95),
            "p99_ms": percentile(success_lat, 99),
            **sys_metrics,
        }
        return summary, lat_rows, sampler.samples

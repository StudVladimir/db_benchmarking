import argparse
import subprocess
import sys


def run_once(base_args: list[str], metric_mode: str) -> None:
    cmd = [
        sys.executable,
        ".\\benchmark_redis_metrics.py",
        "--metric-mode",
        metric_mode,
        *base_args,
    ]
    print("Running:", " ".join(cmd))
    proc = subprocess.run(cmd, text=True)
    if proc.returncode != 0:
        raise SystemExit(f"Run failed for metric-mode={metric_mode} with code {proc.returncode}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run Redis metric benchmarks in three passes: client, server, redis.exec")
    p.add_argument("--query-mode", choices=["mixed", "sequential"], default="mixed")
    p.add_argument("--memory-limit", default="2g")
    p.add_argument("--cpus", type=float, default=1.0)
    p.add_argument("--concurrency", type=int, default=1)
    p.add_argument("--warmup-requests", type=int, default=200)
    p.add_argument("--benchmark-requests", type=int, default=11000)
    p.add_argument("--per-query-requests", type=int, default=1000)
    p.add_argument("--stats-interval-sec", type=float, default=1.0)
    p.add_argument("--results-dir", default="results_redis_metrics")
    p.add_argument("--label", default="")

    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=6379)
    p.add_argument("--db", type=int, default=0)
    p.add_argument("--container", default="dbb-redis")
    p.add_argument("--scan-page-size", type=int, default=1000)

    p.add_argument("--max-user-id", type=int, default=10000)
    p.add_argument("--max-seller-id", type=int, default=10000)
    p.add_argument("--max-order-id", type=int, default=10000)

    p.add_argument("--no-recreate-container", dest="recreate_container", action="store_false")
    p.set_defaults(recreate_container=True)
    return p.parse_args()


def main() -> None:
    args = parse_args()

    base_args = [
        "--query-mode",
        args.query_mode,
        "--memory-limit",
        args.memory_limit,
        "--cpus",
        str(args.cpus),
        "--concurrency",
        str(args.concurrency),
        "--warmup-requests",
        str(args.warmup_requests),
        "--benchmark-requests",
        str(args.benchmark_requests),
        "--per-query-requests",
        str(args.per_query_requests),
        "--stats-interval-sec",
        str(args.stats_interval_sec),
        "--results-dir",
        args.results_dir,
        "--label",
        args.label,
        "--host",
        args.host,
        "--port",
        str(args.port),
        "--db",
        str(args.db),
        "--container",
        args.container,
        "--scan-page-size",
        str(args.scan_page_size),
        "--max-user-id",
        str(args.max_user_id),
        "--max-seller-id",
        str(args.max_seller_id),
        "--max-order-id",
        str(args.max_order_id),
    ]

    if not args.recreate_container:
        base_args.append("--no-recreate-container")

    run_once(base_args, "client")
    run_once(base_args, "server")
    run_once(base_args, "redis.exec")


if __name__ == "__main__":
    main()

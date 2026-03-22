import argparse
import csv
import math
import re
import statistics
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from benchmark_mysql import build_queries


RE_ACTUAL_TIME = re.compile(r"actual time=([0-9.]+)\.\.([0-9.]+)")
RE_ROWS = re.compile(r"rows=([0-9]+)")


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


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def connect_mysql(host: str, port: int, user: str, password: str, database: str) -> Any:
    try:
        import pymysql
    except ImportError as ex:
        raise RuntimeError("Install dependency: pip install pymysql") from ex

    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        autocommit=True,
        charset="utf8mb4",
    )


def get_logins(conn: Any) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT Login FROM Users ORDER BY idUser")
        return [row[0] for row in cur.fetchall()]


def explain_analyze_mysql(conn: Any, sql: str, params: tuple[Any, ...]) -> tuple[float, int]:
    with conn.cursor() as cur:
        cur.execute("EXPLAIN ANALYZE " + sql, params)
        lines = [row[0] for row in cur.fetchall()]

    if not lines:
        return float("nan"), 0

    first = lines[0]
    tmatch = RE_ACTUAL_TIME.search(first)
    rmatch = RE_ROWS.search(first)

    if tmatch:
        server_ms = float(tmatch.group(2))
    else:
        server_ms = float("nan")

    rows_out = int(rmatch.group(1)) if rmatch else 0
    return server_ms, rows_out


def summarize_phase(db: str, phase: str, rows: list[dict[str, Any]], elapsed_sec: float) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    qnames = sorted({x["query_name"] for x in rows})
    for qn in qnames:
        q_rows = [x for x in rows if x["query_name"] == qn]
        ok_rows = [x for x in q_rows if x["ok"]]
        tms = [x["server_time_ms"] for x in ok_rows if not math.isnan(x["server_time_ms"])]
        returned = [x["rows_returned"] for x in ok_rows]
        out.append(
            {
                "db": db,
                "phase": phase,
                "query_name": qn,
                "duration_sec": elapsed_sec,
                "total_requests": len(q_rows),
                "success_requests": len(ok_rows),
                "error_requests": len(q_rows) - len(ok_rows),
                "ops_per_sec": (len(q_rows) / elapsed_sec) if elapsed_sec > 0 else float("nan"),
                "server_p50_ms": percentile(tms, 50),
                "server_p95_ms": percentile(tms, 95),
                "server_p99_ms": percentile(tms, 99),
                "server_avg_ms": statistics.fmean(tms) if tms else float("nan"),
                "avg_rows_returned": statistics.fmean(returned) if returned else float("nan"),
                "total_rows_returned": sum(returned) if returned else 0,
            }
        )
    return out


def run_phase(
    conn: Any,
    phase: str,
    query_defs: list[Any],
    total_requests: int,
    seed: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    details: list[dict[str, Any]] = []
    q_count = len(query_defs)
    t0 = time.perf_counter()

    for i in range(total_requests):
        qdef = query_defs[i % q_count]
        seq = i // q_count
        params = qdef.params_factory(seq, seed)
        ok = True
        err = ""
        server_ms = float("nan")
        rows_ret = 0
        try:
            server_ms, rows_ret = explain_analyze_mysql(conn, qdef.sql, params)
        except Exception as ex:
            ok = False
            err = str(ex)

        details.append(
            {
                "phase": phase,
                "request_index": i,
                "query_name": qdef.name,
                "server_time_ms": server_ms,
                "rows_returned": rows_ret,
                "ok": ok,
                "error": err,
            }
        )

    elapsed = time.perf_counter() - t0
    summary = summarize_phase("mysql", phase, details, elapsed)
    return summary, details


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MySQL server-time benchmark (separate from client-latency runs)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=3306)
    p.add_argument("--user", default="root")
    p.add_argument("--password", default="root")
    p.add_argument("--database", default="mydb")

    p.add_argument("--query-mode", choices=["mixed", "sequential"], default="sequential")
    p.add_argument("--warmup-requests", type=int, default=200)
    p.add_argument("--benchmark-requests", type=int, default=11000)
    p.add_argument("--per-query-requests", type=int, default=1000)

    p.add_argument("--max-user-id", type=int, default=10000)
    p.add_argument("--max-seller-id", type=int, default=10000)
    p.add_argument("--max-order-id", type=int, default=10000)

    p.add_argument("--results-dir", default="results_server_time")
    p.add_argument("--label", default="")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    ns = SimpleNamespace(
        max_user_id=args.max_user_id,
        max_seller_id=args.max_seller_id,
        max_order_id=args.max_order_id,
    )
    query_defs = build_queries(ns)

    conn = connect_mysql(args.host, args.port, args.user, args.password, args.database)
    try:
        logins = get_logins(conn)
        if not logins:
            raise RuntimeError("Users table is empty")
        seed = {"logins": logins}

        warmup_summary, warmup_details = run_phase(
            conn=conn,
            phase="warmup",
            query_defs=query_defs,
            total_requests=args.warmup_requests,
            seed=seed,
        )

        bench_summary: list[dict[str, Any]] = []
        bench_details: list[dict[str, Any]] = []
        if args.query_mode == "sequential":
            for q in query_defs:
                s, d = run_phase(conn, "benchmark", [q], args.per_query_requests, seed)
                bench_summary.extend(s)
                bench_details.extend(d)
        else:
            bench_summary, bench_details = run_phase(
                conn=conn,
                phase="benchmark",
                query_defs=query_defs,
                total_requests=args.benchmark_requests,
                seed=seed,
            )
    finally:
        conn.close()

    stamp = time.strftime("%Y%m%d_%H%M%S")
    suffix = f"_{args.label}" if args.label else ""
    out_dir = Path(args.results_dir) / "mysql" / f"{stamp}_{args.query_mode}{suffix}"
    out_dir.mkdir(parents=True, exist_ok=True)

    summary_rows = warmup_summary + bench_summary
    detail_rows = warmup_details + bench_details

    write_csv(out_dir / "server_time_summary.csv", summary_rows)
    write_csv(out_dir / "server_time_details.csv", detail_rows)

    print(f"Completed. Results saved to: {out_dir}")


if __name__ == "__main__":
    main()

import argparse
import csv
import json
import math
import statistics
import subprocess
import threading
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from benchmark_postgres import build_queries


class PsqlCliConnection:
    def __init__(self, container: str, user: str, dbname: str):
        self.lock = threading.Lock()
        self.proc = subprocess.Popen(
            [
                "docker",
                "exec",
                "-i",
                container,
                "psql",
                "-U",
                user,
                "-d",
                dbname,
                "-At",
                "-F",
                "|",
                "-q",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

    def close(self) -> None:
        if self.proc.stdin:
            try:
                self.proc.stdin.write("\\q\n")
                self.proc.stdin.flush()
            except Exception:
                pass
        self.proc.terminate()
        self.proc.wait(timeout=5)

    def _quote_value(self, value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, (int, float)):
            return str(value)
        text = str(value).replace("'", "''")
        return f"'{text}'"

    def _render_sql(self, sql: str, params: tuple[Any, ...]) -> str:
        parts = sql.split("%s")
        if len(parts) - 1 != len(params):
            raise ValueError(f"Placeholder count mismatch for SQL: {sql}")
        out = [parts[0]]
        for i, p in enumerate(params):
            out.append(self._quote_value(p))
            out.append(parts[i + 1])
        return "".join(out)

    def execute(self, sql: str, params: tuple[Any, ...]) -> list[list[str]]:
        marker = f"__END_{uuid.uuid4().hex}__"
        rendered = self._render_sql(sql, params)
        payload = f"{rendered}\nSELECT '{marker}';\n"

        with self.lock:
            if self.proc.stdin is None or self.proc.stdout is None:
                raise RuntimeError("psql process pipes are not available")

            self.proc.stdin.write(payload)
            self.proc.stdin.flush()

            rows: list[list[str]] = []
            while True:
                line = self.proc.stdout.readline()
                if line == "":
                    stderr = ""
                    if self.proc.stderr is not None:
                        stderr = self.proc.stderr.read()
                    raise RuntimeError(f"psql session ended unexpectedly. STDERR: {stderr}")
                line = line.rstrip("\r\n")
                if line == marker:
                    break
                if line:
                    rows.append(next(csv.reader([line], delimiter="|")))
            return rows


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


def get_logins(conn: PsqlCliConnection) -> list[str]:
    rows = conn.execute('SELECT "Login" FROM mydb."Users" ORDER BY "idUser";', tuple())
    return [row[0] for row in rows if row]


def explain_analyze_postgres(conn: PsqlCliConnection, sql: str, params: tuple[Any, ...]) -> tuple[float, int]:
    rows = conn.execute(f"EXPLAIN (ANALYZE, FORMAT JSON) {sql}", params)
    if not rows:
        return float("nan"), 0

    doc = json.loads(rows[0][0])
    node = doc[0]
    exec_ms = float(node.get("Execution Time", float("nan")))
    rows_out = int(node.get("Plan", {}).get("Actual Rows", 0))
    return exec_ms, rows_out


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
    conn: PsqlCliConnection,
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
            server_ms, rows_ret = explain_analyze_postgres(conn, qdef.sql, params)
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
    summary = summarize_phase("postgres", phase, details, elapsed)
    return summary, details


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="PostgreSQL server-time benchmark (separate from client-latency runs)")
    p.add_argument("--container", default="dbb-postgres")
    p.add_argument("--user", default="benchmark")
    p.add_argument("--database", default="benchmark")

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

    conn = PsqlCliConnection(args.container, args.user, args.database)
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
    out_dir = Path(args.results_dir) / "postgres" / f"{stamp}_{args.query_mode}{suffix}"
    out_dir.mkdir(parents=True, exist_ok=True)

    summary_rows = warmup_summary + bench_summary
    detail_rows = warmup_details + bench_details

    write_csv(out_dir / "server_time_summary.csv", summary_rows)
    write_csv(out_dir / "server_time_details.csv", detail_rows)

    print(f"Completed. Results saved to: {out_dir}")


if __name__ == "__main__":
    main()

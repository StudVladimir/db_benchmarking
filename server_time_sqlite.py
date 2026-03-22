import argparse
import csv
import math
import queue
import re
import statistics
import subprocess
import threading
import time
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from benchmark_sqlite import build_queries


RE_TIMER = re.compile(r"Run Time:\s+real\s+([0-9.]+)")


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


class SqliteTimedCli:
    def __init__(self, container: str, db_path: str):
        self.container = container
        self.db_path = db_path
        self.lock = threading.Lock()
        self.proc = subprocess.Popen(
            ["docker", "exec", "-i", container, "sqlite3", db_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self.timer_queue: queue.Queue[str] = queue.Queue()
        self._stderr_thread = threading.Thread(target=self._collect_stderr, daemon=True)
        self._stderr_thread.start()

        self._send_raw(".timer on")
        self._send_raw(".mode list")
        self._send_raw(".headers off")
        self._send_raw("PRAGMA foreign_keys = ON;")

    def close(self) -> None:
        if self.proc.stdin:
            try:
                self.proc.stdin.write(".quit\n")
                self.proc.stdin.flush()
            except Exception:
                pass
        self.proc.terminate()
        self.proc.wait(timeout=5)

    def _collect_stderr(self) -> None:
        if self.proc.stderr is None:
            return
        while True:
            line = self.proc.stderr.readline()
            if line == "":
                break
            self.timer_queue.put(line.rstrip("\r\n"))

    def _drain_timer_queue(self) -> None:
        try:
            while True:
                self.timer_queue.get_nowait()
        except queue.Empty:
            return

    def _collect_timer_ms(self, timeout_sec: float = 0.2) -> float:
        lines: list[str] = []
        deadline = time.time() + timeout_sec
        while time.time() < deadline:
            remaining = deadline - time.time()
            try:
                lines.append(self.timer_queue.get(timeout=max(0.01, remaining)))
            except queue.Empty:
                break

        for line in lines:
            m = RE_TIMER.search(line)
            if m:
                return float(m.group(1)) * 1000.0
        return float("nan")

    def _quote(self, value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, (int, float)):
            return str(value)
        txt = str(value).replace("'", "''")
        return f"'{txt}'"

    def render_sql(self, sql: str, params: tuple[Any, ...]) -> str:
        parts = sql.split("?")
        if len(parts) - 1 != len(params):
            raise ValueError(f"Placeholder count mismatch for SQL: {sql}")
        out = [parts[0]]
        for i, p in enumerate(params):
            out.append(self._quote(p))
            out.append(parts[i + 1])
        rendered = "".join(out).strip()
        if not rendered.endswith(";"):
            rendered += ";"
        return rendered

    def _exec(self, sql: str, suppress_output: bool) -> tuple[list[str], float]:
        marker = f"__END_{uuid.uuid4().hex}__"
        self._drain_timer_queue()

        cmd_lines = []
        if suppress_output:
            cmd_lines.append(".output /dev/null")
        cmd_lines.append(sql)
        if suppress_output:
            cmd_lines.append(".output stdout")
        cmd_lines.append(f"SELECT '{marker}';")
        payload = "\n".join(cmd_lines) + "\n"

        outputs: list[str] = []
        with self.lock:
            if self.proc.stdin is None or self.proc.stdout is None:
                raise RuntimeError("sqlite3 process pipes are not available")
            self.proc.stdin.write(payload)
            self.proc.stdin.flush()

            while True:
                line = self.proc.stdout.readline()
                if line == "":
                    raise RuntimeError("sqlite3 process ended unexpectedly")
                line = line.rstrip("\r\n")
                if line == marker:
                    break
                if line:
                    outputs.append(line)

        timer_ms = self._collect_timer_ms()
        return outputs, timer_ms

    def _send_raw(self, cmd: str) -> None:
        self._exec(cmd if cmd.endswith(";") or cmd.startswith(".") else cmd + ";", suppress_output=True)

    def execute_timed(self, sql: str, params: tuple[Any, ...]) -> tuple[float, int]:
        rendered = self.render_sql(sql, params)
        _, timer_ms = self._exec(rendered, suppress_output=True)

        inner = rendered.rstrip(";")
        count_sql = f"SELECT COUNT(*) FROM ({inner}) AS _t;"
        out, _ = self._exec(count_sql, suppress_output=False)
        rows_ret = int(out[0]) if out else 0
        return timer_ms, rows_ret


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
    cli: SqliteTimedCli,
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
            server_ms, rows_ret = cli.execute_timed(qdef.sql, params)
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
    summary = summarize_phase("sqlite", phase, details, elapsed)
    return summary, details


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SQLite server-time benchmark (separate from client-latency runs)")
    p.add_argument("--container", default="dbb-sqlite")
    p.add_argument("--db-path", default="/data/benchmark.db")

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

    cli = SqliteTimedCli(args.container, args.db_path)
    try:
        # Reuse query params list from existing table values.
        _, users_timer = cli._exec('SELECT COUNT(*) FROM "Users";', suppress_output=False)
        if math.isnan(users_timer):
            pass
        login_out, _ = cli._exec('SELECT "Login" FROM "Users" ORDER BY "idUser";', suppress_output=False)
        if not login_out:
            raise RuntimeError("Users table is empty")
        seed = {"logins": login_out}

        warmup_summary, warmup_details = run_phase(
            cli=cli,
            phase="warmup",
            query_defs=query_defs,
            total_requests=args.warmup_requests,
            seed=seed,
        )

        bench_summary: list[dict[str, Any]] = []
        bench_details: list[dict[str, Any]] = []
        if args.query_mode == "sequential":
            for q in query_defs:
                s, d = run_phase(cli, "benchmark", [q], args.per_query_requests, seed)
                bench_summary.extend(s)
                bench_details.extend(d)
        else:
            bench_summary, bench_details = run_phase(
                cli=cli,
                phase="benchmark",
                query_defs=query_defs,
                total_requests=args.benchmark_requests,
                seed=seed,
            )
    finally:
        cli.close()

    stamp = time.strftime("%Y%m%d_%H%M%S")
    suffix = f"_{args.label}" if args.label else ""
    out_dir = Path(args.results_dir) / "sqlite" / f"{stamp}_{args.query_mode}{suffix}"
    out_dir.mkdir(parents=True, exist_ok=True)

    write_csv(out_dir / "server_time_summary.csv", warmup_summary + bench_summary)
    write_csv(out_dir / "server_time_details.csv", warmup_details + bench_details)

    print(f"Completed. Results saved to: {out_dir}")


if __name__ == "__main__":
    main()

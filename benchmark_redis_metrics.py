import argparse
import datetime as dt
import math
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from benchmark_engine import (
    DockerStatsSampler,
    ensure_single_active_service,
    percentile,
    summarize_docker_stats,
    write_csv,
)


@dataclass
class RedisQueryDef:
    name: str
    sql: str
    params_factory: Callable[[int, dict[str, Any]], tuple[Any, ...]]


class RedisSqlLikeExecutor:
    def __init__(self, client: Any, scan_page_size: int = 1000):
        self.client = client
        self.scan_page_size = scan_page_size

    def _t(self, fn: Callable[[], Any]) -> tuple[Any, float]:
        t0 = time.perf_counter()
        result = fn()
        return result, (time.perf_counter() - t0) * 1000.0

    def _scan_keys(self, pattern: str) -> tuple[list[str], float]:
        keys: list[str] = []

        def run() -> None:
            for key in self.client.scan_iter(match=pattern, count=self.scan_page_size):
                if isinstance(key, bytes):
                    keys.append(key.decode("utf-8"))
                else:
                    keys.append(str(key))

        _, t_ms = self._t(run)
        return keys, t_ms

    def _hgetall(self, key: str) -> tuple[dict[str, str], float]:
        raw, t_ms = self._t(lambda: self.client.hgetall(key))
        row: dict[str, str] = {}
        for k, v in raw.items():
            kk = k.decode("utf-8") if isinstance(k, bytes) else str(k)
            vv = v.decode("utf-8") if isinstance(v, bytes) else str(v)
            row[kk] = vv
        return row, t_ms

    def _smembers(self, key: str) -> tuple[list[str], float]:
        raw, t_ms = self._t(lambda: self.client.smembers(key))
        out: list[str] = []
        for x in raw:
            out.append(x.decode("utf-8") if isinstance(x, bytes) else str(x))
        return out, t_ms

    @staticmethod
    def _parse_int(value: str, default: int = 0) -> int:
        try:
            return int(value)
        except Exception:
            return default

    @staticmethod
    def _parse_float(value: str, default: float = 0.0) -> float:
        try:
            return float(value)
        except Exception:
            return default

    def execute(self, query_name: str, params: tuple[Any, ...], seed: dict[str, Any]) -> tuple[list[dict[str, str]], float, float]:
        started = time.perf_counter()
        redis_exec_ms = 0.0

        def redis_call(fn: Callable[[], Any]) -> Any:
            nonlocal redis_exec_ms
            result, t_ms = self._t(fn)
            redis_exec_ms += t_ms
            return result

        rows: list[dict[str, str]] = []

        if query_name == "q1_user_by_id":
            user_id = int(params[0])
            row = redis_call(lambda: self._hgetall(f"Users:{user_id}"))[0]
            if row:
                rows = [row]

        elif query_name == "q2_all_users":
            user_keys = redis_call(lambda: self._scan_keys("Users:*"))[0]
            user_ids = [k.split(":", 1)[1] for k in user_keys if ":" in k]
            user_ids.sort(key=lambda x: self._parse_int(x, 0))
            for uid in user_ids:
                row = redis_call(lambda u=uid: self._hgetall(f"Users:{u}"))[0]
                if row:
                    rows.append(row)

        elif query_name == "q3_user_by_login":
            login = str(params[0])
            ids = redis_call(lambda: self._smembers(f"idx:Users:Login:{login}"))[0]
            for uid in ids:
                row = redis_call(lambda u=uid: self._hgetall(f"Users:{u}"))[0]
                if row:
                    rows.append(row)

        elif query_name == "q4_sellers_rating":
            ids_set: set[str] = set()
            for rating in (4, 5):
                ids = redis_call(lambda r=rating: self._smembers(f"idx:Sellers:Rating:{r}"))[0]
                ids_set.update(ids)
            ids = sorted(ids_set, key=lambda x: self._parse_int(x, 0))[:100]
            for sid in ids:
                row = redis_call(lambda s=sid: self._hgetall(f"Sellers:{s}"))[0]
                if row:
                    rows.append(row)

        elif query_name == "q5_orders_offset":
            order_keys = redis_call(lambda: self._scan_keys("Orders:*"))[0]
            order_ids = [k.split(":", 1)[1] for k in order_keys if ":" in k]
            order_ids.sort(key=lambda x: self._parse_int(x, 0))
            sliced = order_ids[5000 : 5000 + 50]
            for oid in sliced:
                row = redis_call(lambda o=oid: self._hgetall(f"Orders:{o}"))[0]
                if row:
                    rows.append(row)

        elif query_name == "q6_products_by_seller":
            seller_id = int(params[0])
            pids = redis_call(lambda: self._smembers(f"idx:Products:Seller_id:{seller_id}"))[0]
            for pid in pids:
                row = redis_call(lambda p=pid: self._hgetall(f"Products:{p}"))[0]
                if row:
                    rows.append(row)

        elif query_name == "q7_products_join_sellers":
            seller_id = int(params[0])
            seller_row = redis_call(lambda: self._hgetall(f"Sellers:{seller_id}"))[0]
            seller_name = seller_row.get("Name", "")
            pids = redis_call(lambda: self._smembers(f"idx:Products:Seller_id:{seller_id}"))[0]
            for pid in pids:
                row = redis_call(lambda p=pid: self._hgetall(f"Products:{p}"))[0]
                if row:
                    merged = dict(row)
                    merged["SellerName"] = seller_name
                    rows.append(merged)

        elif query_name == "q8_order_items_join_products":
            order_id = int(params[0])
            item_ids = redis_call(lambda: self._smembers(f"idx:Order_Items:Order_id:{order_id}"))[0]
            for item_id in item_ids:
                item = redis_call(lambda i=item_id: self._hgetall(f"Order_Items:{i}"))[0]
                if not item:
                    continue
                pid = item.get("Product_id", "")
                prod = redis_call(lambda p=pid: self._hgetall(f"Products:{p}"))[0]
                if not prod:
                    continue
                rows.append(
                    {
                        "Product_id": item.get("Product_id", ""),
                        "Quantity": item.get("Quantity", ""),
                        "Title": prod.get("Title", ""),
                        "Price": prod.get("Price", ""),
                    }
                )

        elif query_name == "q9_orders_items_products_by_user":
            user_id = int(params[0])
            order_ids = redis_call(lambda: self._smembers(f"idx:Orders:Users_id:{user_id}"))[0]
            for oid in order_ids:
                order = redis_call(lambda o=oid: self._hgetall(f"Orders:{o}"))[0]
                if not order:
                    continue
                item_ids = redis_call(lambda o=oid: self._smembers(f"idx:Order_Items:Order_id:{o}"))[0]
                for item_id in item_ids:
                    item = redis_call(lambda i=item_id: self._hgetall(f"Order_Items:{i}"))[0]
                    if not item:
                        continue
                    pid = item.get("Product_id", "")
                    prod = redis_call(lambda p=pid: self._hgetall(f"Products:{p}"))[0]
                    if not prod:
                        continue
                    rows.append(
                        {
                            "idOrder": oid,
                            "Create_Date": order.get("Create_Date", ""),
                            "Product_id": item.get("Product_id", ""),
                            "Quantity": item.get("Quantity", ""),
                            "Title": prod.get("Title", ""),
                            "Price": prod.get("Price", ""),
                        }
                    )

        elif query_name == "q10_top_sellers_qty":
            product_seller = seed["product_seller"]
            item_keys = redis_call(lambda: self._scan_keys("Order_Items:*"))[0]
            agg: dict[str, tuple[int, float]] = {}
            for item_key in item_keys:
                item = redis_call(lambda k=item_key: self._hgetall(k))[0]
                if not item:
                    continue
                pid = item.get("Product_id", "")
                seller_id = product_seller.get(pid)
                if not seller_id:
                    continue
                qty = self._parse_float(item.get("Quantity", "0"), 0.0)
                prev = agg.get(seller_id, (0, 0.0))
                agg[seller_id] = (prev[0] + 1, prev[1] + qty)
            top = sorted(agg.items(), key=lambda x: x[1][1], reverse=True)[:20]
            for seller_id, (items_count, qty_sum) in top:
                rows.append(
                    {
                        "Seller_id": seller_id,
                        "items": str(items_count),
                        "qty": f"{qty_sum:.6f}",
                    }
                )

        elif query_name == "q11_top_categories":
            pc_keys = redis_call(lambda: self._scan_keys("Product_Categories:*"))[0]
            counts: dict[str, int] = {}
            for key in pc_keys:
                row = redis_call(lambda k=key: self._hgetall(k))[0]
                if not row:
                    continue
                cid = row.get("Category_id", "")
                if not cid:
                    continue
                counts[cid] = counts.get(cid, 0) + 1
            top = sorted(counts.items(), key=lambda x: x[1], reverse=True)[:10]
            for cid, cnt in top:
                rows.append({"Category_id": cid, "cnt": str(cnt)})

        else:
            raise ValueError(f"Unknown query: {query_name}")

        server_ms = (time.perf_counter() - started) * 1000.0
        return rows, server_ms, redis_exec_ms


def wait_redis_ready(host: str, port: int, db: int, timeout_sec: int = 60) -> None:
    try:
        import redis
    except ImportError as ex:
        raise RuntimeError("Install dependency: pip install redis") from ex

    deadline = time.time() + timeout_sec
    last_error = ""
    while time.time() < deadline:
        try:
            client = redis.Redis(host=host, port=port, db=db, decode_responses=False)
            if client.ping():
                return
        except Exception as ex:
            last_error = str(ex)
        time.sleep(1)
    raise RuntimeError(f"Redis did not become ready within timeout: {last_error}")


def connect_redis(host: str, port: int, db: int) -> Any:
    try:
        import redis
    except ImportError as ex:
        raise RuntimeError("Install dependency: pip install redis") from ex

    return redis.Redis(host=host, port=port, db=db, decode_responses=False)


def load_seed_data(client: Any, max_scan_count: int) -> dict[str, Any]:
    logins: list[str] = []
    for key in client.scan_iter(match="idx:Users:Login:*", count=max_scan_count):
        k = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        logins.append(k.split("idx:Users:Login:", 1)[1])

    if not logins:
        # Fallback path if login index is absent.
        for key in client.scan_iter(match="Users:*", count=max_scan_count):
            kk = key.decode("utf-8") if isinstance(key, bytes) else str(key)
            row = client.hgetall(kk)
            login_raw = row.get(b"Login") or row.get("Login")
            if login_raw:
                login = login_raw.decode("utf-8") if isinstance(login_raw, bytes) else str(login_raw)
                logins.append(login)

    if not logins:
        raise RuntimeError("Users table/login index appears empty in Redis")

    product_seller: dict[str, str] = {}
    for key in client.scan_iter(match="Products:*", count=max_scan_count):
        kk = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        pid = kk.split(":", 1)[1] if ":" in kk else kk
        row = client.hgetall(kk)
        seller_raw = row.get(b"Seller_id") or row.get("Seller_id")
        if seller_raw:
            seller = seller_raw.decode("utf-8") if isinstance(seller_raw, bytes) else str(seller_raw)
            product_seller[pid] = seller

    return {
        "logins": logins,
        "product_seller": product_seller,
    }


def build_queries(args: argparse.Namespace) -> list[RedisQueryDef]:
    return [
        RedisQueryDef(
            name="q1_user_by_id",
            sql="SELECT * FROM Users WHERE idUser = ?;",
            params_factory=lambda seq, _: ((seq % args.max_user_id) + 1,),
        ),
        RedisQueryDef(
            name="q2_all_users",
            sql="SELECT * FROM Users;",
            params_factory=lambda seq, _: tuple(),
        ),
        RedisQueryDef(
            name="q3_user_by_login",
            sql="SELECT * FROM Users WHERE Login = ?;",
            params_factory=lambda seq, seed: (seed["logins"][seq % len(seed["logins"])],),
        ),
        RedisQueryDef(
            name="q4_sellers_rating",
            sql="SELECT * FROM Sellers WHERE Rating >= 4 LIMIT 100;",
            params_factory=lambda seq, _: tuple(),
        ),
        RedisQueryDef(
            name="q5_orders_offset",
            sql="SELECT * FROM Orders ORDER BY idOrder LIMIT 50 OFFSET 5000;",
            params_factory=lambda seq, _: tuple(),
        ),
        RedisQueryDef(
            name="q6_products_by_seller",
            sql="SELECT * FROM Products WHERE Seller_id = ?;",
            params_factory=lambda seq, _: ((seq % args.max_seller_id) + 1,),
        ),
        RedisQueryDef(
            name="q7_products_join_sellers",
            sql=(
                "SELECT p.*, s.Name "
                "FROM Products p "
                "JOIN Sellers s ON s.idSeller = p.Seller_id "
                "WHERE s.idSeller = ?;"
            ),
            params_factory=lambda seq, _: ((seq % args.max_seller_id) + 1,),
        ),
        RedisQueryDef(
            name="q8_order_items_join_products",
            sql=(
                "SELECT oi.Product_id, oi.Quantity, p.Title, p.Price "
                "FROM Order_Items oi "
                "JOIN Products p ON p.idProduct = oi.Product_id "
                "WHERE oi.Order_id = ?;"
            ),
            params_factory=lambda seq, _: ((seq % args.max_order_id) + 1,),
        ),
        RedisQueryDef(
            name="q9_orders_items_products_by_user",
            sql=(
                "SELECT o.idOrder, o.Create_Date, oi.Product_id, oi.Quantity, p.Title, p.Price "
                "FROM Orders o "
                "JOIN Order_Items oi ON oi.Order_id = o.idOrder "
                "JOIN Products p ON p.idProduct = oi.Product_id "
                "WHERE o.Users_id = ?;"
            ),
            params_factory=lambda seq, _: ((seq % args.max_user_id) + 1,),
        ),
        RedisQueryDef(
            name="q10_top_sellers_qty",
            sql=(
                "SELECT p.Seller_id, COUNT(*) AS items, SUM(oi.Quantity) AS qty "
                "FROM Order_Items oi "
                "JOIN Products p ON p.idProduct = oi.Product_id "
                "GROUP BY p.Seller_id "
                "ORDER BY qty DESC "
                "LIMIT 20;"
            ),
            params_factory=lambda seq, _: tuple(),
        ),
        RedisQueryDef(
            name="q11_top_categories",
            sql=(
                "SELECT pc.Category_id, COUNT(*) cnt "
                "FROM Product_Categories pc "
                "GROUP BY pc.Category_id "
                "ORDER BY cnt DESC "
                "LIMIT 10;"
            ),
            params_factory=lambda seq, _: tuple(),
        ),
    ]


def summarize_phase(
    db: str,
    phase: str,
    rows: list[dict[str, Any]],
    elapsed_sec: float,
    metric_mode: str,
    sys_metrics: dict[str, Any],
) -> list[dict[str, Any]]:
    metric_col = {
        "client": "client_time_ms",
        "server": "server_time_ms",
        "redis.exec": "redis_exec_ms",
    }[metric_mode]

    out: list[dict[str, Any]] = []
    for qn in sorted({x["query_name"] for x in rows}):
        q_rows = [x for x in rows if x["query_name"] == qn]
        ok_rows = [x for x in q_rows if x["ok"]]
        metric_values = [x[metric_col] for x in ok_rows if not math.isnan(x[metric_col])]
        client_values = [x["client_time_ms"] for x in ok_rows if not math.isnan(x["client_time_ms"])]
        server_values = [x["server_time_ms"] for x in ok_rows if not math.isnan(x["server_time_ms"])]
        redis_values = [x["redis_exec_ms"] for x in ok_rows if not math.isnan(x["redis_exec_ms"])]
        returned = [x["rows_returned"] for x in ok_rows]

        out.append(
            {
                "db": db,
                "phase": phase,
                "metric_mode": metric_mode,
                "query_name": qn,
                "duration_sec": elapsed_sec,
                "total_requests": len(q_rows),
                "success_requests": len(ok_rows),
                "error_requests": len(q_rows) - len(ok_rows),
                "ops_per_sec": (len(q_rows) / elapsed_sec) if elapsed_sec > 0 else float("nan"),
                "p50_ms": percentile(metric_values, 50),
                "p95_ms": percentile(metric_values, 95),
                "p99_ms": percentile(metric_values, 99),
                "avg_metric_ms": statistics.fmean(metric_values) if metric_values else float("nan"),
                "avg_client_ms": statistics.fmean(client_values) if client_values else float("nan"),
                "avg_server_ms": statistics.fmean(server_values) if server_values else float("nan"),
                "avg_redis_exec_ms": statistics.fmean(redis_values) if redis_values else float("nan"),
                "avg_rows_returned": statistics.fmean(returned) if returned else float("nan"),
                "total_rows_returned": sum(returned) if returned else 0,
                **sys_metrics,
            }
        )

    return out


def run_phase(
    phase: str,
    total_requests: int,
    concurrency: int,
    query_defs: list[RedisQueryDef],
    seed: dict[str, Any],
    stats_sampler: DockerStatsSampler,
    connect_factory: Callable[[], Any],
    scan_page_size: int,
    metric_mode: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    lat_rows: list[dict[str, Any]] = []
    lat_lock = threading.Lock()
    conns: list[Any] = []
    conn_lock = threading.Lock()
    thread_local = threading.local()
    q_count = len(query_defs)

    def get_client() -> Any:
        client = getattr(thread_local, "client", None)
        executor = getattr(thread_local, "executor", None)
        if client is None or executor is None:
            client = connect_factory()
            executor = RedisSqlLikeExecutor(client, scan_page_size=scan_page_size)
            thread_local.client = client
            thread_local.executor = executor
            with conn_lock:
                conns.append(client)
        return executor

    def execute_one(i: int) -> None:
        qdef = query_defs[i % q_count]
        seq = i // q_count
        params = qdef.params_factory(seq, seed)

        ok = True
        err = ""
        client_ms = float("nan")
        server_ms = float("nan")
        redis_exec_ms = float("nan")
        rows_ret = 0

        try:
            executor = get_client()
            started = time.perf_counter()
            rows, server_ms, redis_exec_ms = executor.execute(qdef.name, params, seed)
            client_ms = (time.perf_counter() - started) * 1000.0
            rows_ret = len(rows)
        except Exception as ex:
            ok = False
            err = str(ex)

        with lat_lock:
            lat_rows.append(
                {
                    "phase": phase,
                    "metric_mode": metric_mode,
                    "request_index": i,
                    "query_name": qdef.name,
                    "sql": qdef.sql,
                    "client_time_ms": client_ms,
                    "server_time_ms": server_ms,
                    "redis_exec_ms": redis_exec_ms,
                    "rows_returned": rows_ret,
                    "ok": ok,
                    "error": err,
                }
            )

    t0 = time.perf_counter()
    stats_sampler.start()
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        list(pool.map(execute_one, range(total_requests)))
    stats_sampler.stop()
    elapsed = time.perf_counter() - t0

    for client in conns:
        try:
            client.close()
        except Exception:
            pass

    sys_metrics = summarize_docker_stats(stats_sampler.samples)
    summary = summarize_phase(
        db="redis",
        phase=phase,
        rows=lat_rows,
        elapsed_sec=elapsed,
        metric_mode=metric_mode,
        sys_metrics=sys_metrics,
    )

    return summary, lat_rows, stats_sampler.samples


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Redis benchmark with separate client/server/redis.exec metric modes")
    p.add_argument("--memory-limit", default="2g", help="Container memory limit, e.g. 2g or 6g")
    p.add_argument("--cpus", type=float, default=1.0)
    p.add_argument("--concurrency", type=int, default=1, help="Concurrent users (threads), e.g. 1 or 10")
    p.add_argument("--warmup-requests", type=int, default=200)
    p.add_argument("--benchmark-requests", type=int, default=11000)
    p.add_argument("--query-mode", choices=["mixed", "sequential"], default="mixed")
    p.add_argument("--per-query-requests", type=int, default=1000)
    p.add_argument("--metric-mode", choices=["client", "server", "redis.exec"], default="client")
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

    ensure_single_active_service(
        target_service="redis",
        target_container=args.container,
        memory_limit=args.memory_limit,
        cpus=args.cpus,
        recreate_container=args.recreate_container,
        wait_ready=lambda: wait_redis_ready(args.host, args.port, args.db),
    )

    connect_factory = lambda: connect_redis(args.host, args.port, args.db)

    seed_client = connect_factory()
    try:
        if seed_client.dbsize() == 0:
            raise RuntimeError(
                "Redis is empty. Load data first into Redis key schema (Table:id + idx:Table:Column:Value)."
            )
        seed = load_seed_data(seed_client, args.scan_page_size)
    finally:
        seed_client.close()

    queries = build_queries(args)

    all_summary: list[dict[str, Any]] = []
    all_details: list[dict[str, Any]] = []
    all_stats: list[dict[str, Any]] = []

    warmup_sampler = DockerStatsSampler(args.container, "warmup", args.stats_interval_sec)
    warmup_summary, warmup_details, warmup_stats = run_phase(
        phase="warmup",
        total_requests=args.warmup_requests,
        concurrency=args.concurrency,
        query_defs=queries,
        seed=seed,
        stats_sampler=warmup_sampler,
        connect_factory=connect_factory,
        scan_page_size=args.scan_page_size,
        metric_mode=args.metric_mode,
    )
    all_summary.extend(warmup_summary)
    all_details.extend(warmup_details)
    all_stats.extend(warmup_stats)

    if args.query_mode == "sequential":
        for q in queries:
            sampler = DockerStatsSampler(args.container, "benchmark", args.stats_interval_sec)
            s, d, st = run_phase(
                phase="benchmark",
                total_requests=args.per_query_requests,
                concurrency=args.concurrency,
                query_defs=[q],
                seed=seed,
                stats_sampler=sampler,
                connect_factory=connect_factory,
                scan_page_size=args.scan_page_size,
                metric_mode=args.metric_mode,
            )
            all_summary.extend(s)
            all_details.extend(d)
            all_stats.extend(st)
    else:
        bench_sampler = DockerStatsSampler(args.container, "benchmark", args.stats_interval_sec)
        s, d, st = run_phase(
            phase="benchmark",
            total_requests=args.benchmark_requests,
            concurrency=args.concurrency,
            query_defs=queries,
            seed=seed,
            stats_sampler=bench_sampler,
            connect_factory=connect_factory,
            scan_page_size=args.scan_page_size,
            metric_mode=args.metric_mode,
        )
        all_summary.extend(s)
        all_details.extend(d)
        all_stats.extend(st)

    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = f"_{args.label}" if args.label else ""
    mode = args.query_mode
    metric_tag = args.metric_mode.replace(".", "_")
    out_dir = Path(args.results_dir) / "redis" / f"{stamp}_{mode}_{metric_tag}_mem-{args.memory_limit}_u-{args.concurrency}{suffix}"
    out_dir.mkdir(parents=True, exist_ok=True)

    write_csv(out_dir / "metric_summary.csv", all_summary)
    write_csv(out_dir / "metric_details.csv", all_details)
    write_csv(out_dir / "docker_stats_samples.csv", all_stats)

    print(f"Completed. Results saved to: {out_dir}")


if __name__ == "__main__":
    main()

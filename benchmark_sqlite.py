import argparse
import csv
import subprocess
import threading
import time
import uuid
from typing import Any

from benchmark_engine import BenchmarkRunner, QueryDef, run_cmd


class SqliteCliConnection:
    def __init__(self, container: str, db_path: str):
        self.container = container
        self.db_path = db_path
        self.lock = threading.Lock()
        self.proc = subprocess.Popen(
            ["docker", "exec", "-i", container, "sqlite3", "-csv", "-noheader", db_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        self._exec_raw("PRAGMA foreign_keys = ON;")

    def close(self) -> None:
        if self.proc.stdin:
            try:
                self.proc.stdin.write(".quit\n")
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
        parts = sql.split("?")
        if len(parts) - 1 != len(params):
            raise ValueError(f"Placeholder count mismatch for SQL: {sql}")
        rendered = [parts[0]]
        for i, p in enumerate(params):
            rendered.append(self._quote_value(p))
            rendered.append(parts[i + 1])
        return "".join(rendered)

    def _exec_raw(self, sql: str) -> list[list[str]]:
        marker = f"__END_{uuid.uuid4().hex}__"
        payload = f"{sql}\nSELECT '{marker}';\n"

        with self.lock:
            if self.proc.stdin is None or self.proc.stdout is None:
                raise RuntimeError("sqlite3 process pipes are not available")

            self.proc.stdin.write(payload)
            self.proc.stdin.flush()

            rows: list[list[str]] = []
            while True:
                line = self.proc.stdout.readline()
                if line == "":
                    stderr = ""
                    if self.proc.stderr is not None:
                        stderr = self.proc.stderr.read()
                    raise RuntimeError(f"sqlite3 session ended unexpectedly. STDERR: {stderr}")
                line = line.rstrip("\r\n")
                if line == marker:
                    break
                if line:
                    rows.append(next(csv.reader([line])))
            return rows

    def execute(self, sql: str, params: tuple[Any, ...]) -> list[Any]:
        rendered = self._render_sql(sql, params)
        return self._exec_raw(rendered)


def connect_sqlite(container: str, db_path: str) -> SqliteCliConnection:
    return SqliteCliConnection(container=container, db_path=db_path)


def execute_sqlite(conn: SqliteCliConnection, sql: str, params: tuple[Any, ...]) -> list[Any]:
    return conn.execute(sql, params)


def close_sqlite(conn: SqliteCliConnection) -> None:
    conn.close()


def load_seed_sqlite(conn: SqliteCliConnection) -> dict[str, Any]:
    rows = conn.execute('SELECT "Login" FROM "Users" ORDER BY "idUser";', tuple())
    logins = [row[0] for row in rows if row]
    if not logins:
        raise RuntimeError("Users table is empty; cannot prepare Login-based query seed.")
    return {"logins": logins}


def wait_sqlite_ready(container: str, db_path: str, timeout_sec: int = 30) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        proc = run_cmd(
            [
                "docker",
                "exec",
                "-i",
                container,
                "sqlite3",
                db_path,
                "SELECT 1;",
            ],
            check=False,
        )
        if proc.returncode == 0:
            return
        time.sleep(1)
    raise RuntimeError("SQLite container did not become ready within timeout")


def build_queries(args: argparse.Namespace) -> list[QueryDef]:
    return [
        QueryDef(
            name="q1_user_by_id",
            sql='SELECT * FROM "Users" WHERE "idUser" = ?;',
            params_factory=lambda seq, _: ((seq % args.max_user_id) + 1,),
        ),
        QueryDef(
            name="q2_all_users",
            sql='SELECT * FROM "Users";',
            params_factory=lambda seq, _: tuple(),
        ),
        QueryDef(
            name="q3_user_by_login",
            sql='SELECT * FROM "Users" WHERE "Login" = ?;',
            params_factory=lambda seq, seed: (seed["logins"][seq % len(seed["logins"])],),
        ),
        QueryDef(
            name="q4_sellers_rating",
            sql='SELECT * FROM "Sellers" WHERE "Rating" >= 4 LIMIT 100;',
            params_factory=lambda seq, _: tuple(),
        ),
        QueryDef(
            name="q5_orders_offset",
            sql='SELECT * FROM "Orders" ORDER BY "idOrder" LIMIT 50 OFFSET 5000;',
            params_factory=lambda seq, _: tuple(),
        ),
        QueryDef(
            name="q6_products_by_seller",
            sql='SELECT * FROM "Products" WHERE "Seller_id" = ?;',
            params_factory=lambda seq, _: ((seq % args.max_seller_id) + 1,),
        ),
        QueryDef(
            name="q7_products_join_sellers",
            sql=(
                'SELECT p.*, s."Name" '
                'FROM "Products" p '
                'JOIN "Sellers" s ON s."idSeller" = p."Seller_id" '
                'WHERE s."idSeller" = ?;'
            ),
            params_factory=lambda seq, _: ((seq % args.max_seller_id) + 1,),
        ),
        QueryDef(
            name="q8_order_items_join_products",
            sql=(
                'SELECT oi."Product_id", oi."Quantity", p."Title", p."Price" '
                'FROM "Order_Items" oi '
                'JOIN "Products" p ON p."idProduct" = oi."Product_id" '
                'WHERE oi."Order_id" = ?;'
            ),
            params_factory=lambda seq, _: ((seq % args.max_order_id) + 1,),
        ),
        QueryDef(
            name="q9_orders_items_products_by_user",
            sql=(
                'SELECT o."idOrder", o."Create_Date", oi."Product_id", oi."Quantity", p."Title", p."Price" '
                'FROM "Orders" o '
                'JOIN "Order_Items" oi ON oi."Order_id" = o."idOrder" '
                'JOIN "Products" p ON p."idProduct" = oi."Product_id" '
                'WHERE o."Users_id" = ?;'
            ),
            params_factory=lambda seq, _: ((seq % args.max_user_id) + 1,),
        ),
        QueryDef(
            name="q10_top_sellers_qty",
            sql=(
                'SELECT p."Seller_id", COUNT(*) AS items, SUM(oi."Quantity") AS qty '
                'FROM "Order_Items" oi '
                'JOIN "Products" p ON p."idProduct" = oi."Product_id" '
                'GROUP BY p."Seller_id" '
                'ORDER BY qty DESC '
                'LIMIT 20;'
            ),
            params_factory=lambda seq, _: tuple(),
        ),
        QueryDef(
            name="q11_top_categories",
            sql=(
                'SELECT pc."Category_id", COUNT(*) cnt '
                'FROM "Product_Categories" pc '
                'GROUP BY pc."Category_id" '
                'ORDER BY cnt DESC '
                'LIMIT 10;'
            ),
            params_factory=lambda seq, _: tuple(),
        ),
    ]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SQLite benchmark runner (container sqlite3 CLI session)")
    p.add_argument("--memory-limit", default="2g", help="Container memory limit, e.g. 2g or 6g")
    p.add_argument("--cpus", type=float, default=1.0)
    p.add_argument("--concurrency", type=int, default=1, help="Concurrent users (threads), e.g. 1 or 10")
    p.add_argument("--warmup-requests", type=int, default=200)
    p.add_argument("--benchmark-requests", type=int, default=11000)
    p.add_argument("--query-mode", choices=["mixed", "sequential"], default="mixed")
    p.add_argument("--per-query-requests", type=int, default=1000)
    p.add_argument("--stats-interval-sec", type=float, default=1.0)
    p.add_argument("--csv-folder", default="data_csv(third)")
    p.add_argument("--results-dir", default="results")

    p.add_argument("--container", default="dbb-sqlite")
    p.add_argument("--db-path", default="/data/benchmark.db")

    p.add_argument("--max-user-id", type=int, default=10000)
    p.add_argument("--max-seller-id", type=int, default=10000)
    p.add_argument("--max-order-id", type=int, default=10000)

    p.add_argument("--skip-load-data", action="store_true")
    p.add_argument("--no-reset-data", dest="reset_data", action="store_false")
    p.set_defaults(reset_data=True)
    p.add_argument("--no-recreate-container", dest="recreate_container", action="store_false")
    p.set_defaults(recreate_container=True)

    return p.parse_args()


def main() -> None:
    args = parse_args()

    runner = BenchmarkRunner(
        db_name="sqlite",
        service_name="sqlite",
        container_name=args.container,
        connect_factory=lambda: connect_sqlite(args.container, args.db_path),
        execute_query=execute_sqlite,
        close_connection=close_sqlite,
        query_defs=build_queries(args),
        import_flags=["-OnlySqlite"],
        load_seed_data=load_seed_sqlite,
        wait_ready=lambda: wait_sqlite_ready(args.container, args.db_path),
    )
    runner.run(args)


if __name__ == "__main__":
    main()

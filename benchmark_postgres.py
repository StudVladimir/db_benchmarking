import argparse
import time
from typing import Any

from benchmark_engine import BenchmarkRunner, QueryDef, run_cmd


def connect_postgres(host: str, port: int, user: str, password: str, dbname: str) -> Any:
    try:
        import psycopg
    except ImportError as ex:
        raise RuntimeError("Install dependency: pip install psycopg[binary]") from ex

    return psycopg.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
        autocommit=True,
    )


def execute_postgres(conn: Any, sql: str, params: tuple[Any, ...]) -> list[Any]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def close_postgres(conn: Any) -> None:
    conn.close()


def load_seed_postgres(conn: Any) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute('SELECT "Login" FROM mydb."Users" ORDER BY "idUser"')
        logins = [row[0] for row in cur.fetchall()]
    if not logins:
        raise RuntimeError("Users table is empty; cannot prepare Login-based query seed.")
    return {"logins": logins}


def wait_postgres_ready(user: str, dbname: str, timeout_sec: int = 90) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        proc = run_cmd(
            [
                "docker",
                "exec",
                "-i",
                "dbb-postgres",
                "pg_isready",
                "-U",
                user,
                "-d",
                dbname,
            ],
            check=False,
        )
        if proc.returncode == 0:
            return
        time.sleep(1)
    raise RuntimeError("PostgreSQL did not become ready within timeout")


def build_queries(args: argparse.Namespace) -> list[QueryDef]:
    return [
        QueryDef(
            name="q1_user_by_id",
            sql='SELECT * FROM mydb."Users" WHERE "idUser" = %s;',
            params_factory=lambda seq, _: ((seq % args.max_user_id) + 1,),
        ),
        QueryDef(
            name="q2_all_users",
            sql='SELECT * FROM mydb."Users";',
            params_factory=lambda seq, _: tuple(),
        ),
        QueryDef(
            name="q3_user_by_login",
            sql='SELECT * FROM mydb."Users" WHERE "Login" = %s;',
            params_factory=lambda seq, seed: (seed["logins"][seq % len(seed["logins"])],),
        ),
        QueryDef(
            name="q4_sellers_rating",
            sql='SELECT * FROM mydb."Sellers" WHERE "Rating" >= 4 LIMIT 100;',
            params_factory=lambda seq, _: tuple(),
        ),
        QueryDef(
            name="q5_orders_offset",
            sql='SELECT * FROM mydb."Orders" ORDER BY "idOrder" LIMIT 50 OFFSET 5000;',
            params_factory=lambda seq, _: tuple(),
        ),
        QueryDef(
            name="q6_products_by_seller",
            sql='SELECT * FROM mydb."Products" WHERE "Seller_id" = %s;',
            params_factory=lambda seq, _: ((seq % args.max_seller_id) + 1,),
        ),
        QueryDef(
            name="q7_products_join_sellers",
            sql=(
                'SELECT p.*, s."Name" '
                'FROM mydb."Products" p '
                'JOIN mydb."Sellers" s ON s."idSeller" = p."Seller_id" '
                'WHERE s."idSeller" = %s;'
            ),
            params_factory=lambda seq, _: ((seq % args.max_seller_id) + 1,),
        ),
        QueryDef(
            name="q8_order_items_join_products",
            sql=(
                'SELECT oi."Product_id", oi."Quantity", p."Title", p."Price" '
                'FROM mydb."Order_Items" oi '
                'JOIN mydb."Products" p ON p."idProduct" = oi."Product_id" '
                'WHERE oi."Order_id" = %s;'
            ),
            params_factory=lambda seq, _: ((seq % args.max_order_id) + 1,),
        ),
        QueryDef(
            name="q9_orders_items_products_by_user",
            sql=(
                'SELECT o."idOrder", o."Create_Date", oi."Product_id", oi."Quantity", p."Title", p."Price" '
                'FROM mydb."Orders" o '
                'JOIN mydb."Order_Items" oi ON oi."Order_id" = o."idOrder" '
                'JOIN mydb."Products" p ON p."idProduct" = oi."Product_id" '
                'WHERE o."Users_id" = %s;'
            ),
            params_factory=lambda seq, _: ((seq % args.max_user_id) + 1,),
        ),
        QueryDef(
            name="q10_top_sellers_qty",
            sql=(
                'SELECT p."Seller_id", COUNT(*) AS items, SUM(oi."Quantity") AS qty '
                'FROM mydb."Order_Items" oi '
                'JOIN mydb."Products" p ON p."idProduct" = oi."Product_id" '
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
                'FROM mydb."Product_Categories" pc '
                'GROUP BY pc."Category_id" '
                'ORDER BY cnt DESC '
                'LIMIT 10;'
            ),
            params_factory=lambda seq, _: tuple(),
        ),
    ]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="PostgreSQL benchmark runner")
    p.add_argument("--memory-limit", default="2g", help="Container memory limit, e.g. 2g or 6g")
    p.add_argument("--cpus", type=float, default=1.0)
    p.add_argument("--concurrency", type=int, default=1, help="Concurrent users (threads), e.g. 1 or 10")
    p.add_argument("--warmup-requests", type=int, default=200)
    p.add_argument("--benchmark-requests", type=int, default=10000)
    p.add_argument("--stats-interval-sec", type=float, default=1.0)
    p.add_argument("--csv-folder", default="data_csv(third)")
    p.add_argument("--results-dir", default="results")

    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=5432)
    p.add_argument("--user", default="benchmark")
    p.add_argument("--password", default="benchmark")
    p.add_argument("--database", default="benchmark")

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
        db_name="postgres",
        service_name="postgres",
        container_name="dbb-postgres",
        connect_factory=lambda: connect_postgres(args.host, args.port, args.user, args.password, args.database),
        execute_query=execute_postgres,
        close_connection=close_postgres,
        query_defs=build_queries(args),
        import_flags=["-SkipMySql", "-SkipSqlite"],
        load_seed_data=load_seed_postgres,
        wait_ready=lambda: wait_postgres_ready(args.user, args.database),
    )
    runner.run(args)


if __name__ == "__main__":
    main()

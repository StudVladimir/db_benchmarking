import argparse
import time
from typing import Any

from benchmark_engine import BenchmarkRunner, QueryDef, run_cmd


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


def execute_mysql(conn: Any, sql: str, params: tuple[Any, ...]) -> list[Any]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def close_mysql(conn: Any) -> None:
    conn.close()


def load_seed_mysql(conn: Any) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute("SELECT Login FROM Users ORDER BY idUser")
        logins = [row[0] for row in cur.fetchall()]
    if not logins:
        raise RuntimeError("Users table is empty; cannot prepare Login-based query seed.")
    return {"logins": logins}


def wait_mysql_ready(user: str, password: str, timeout_sec: int = 90) -> None:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        proc = run_cmd(
            [
                "docker",
                "exec",
                "-i",
                "dbb-mysql",
                "mysqladmin",
                "ping",
                "-h",
                "127.0.0.1",
                f"-u{user}",
                f"-p{password}",
                "--silent",
            ],
            check=False,
        )
        if proc.returncode == 0:
            return
        time.sleep(1)
    raise RuntimeError("MySQL did not become ready within timeout")


def build_queries(args: argparse.Namespace) -> list[QueryDef]:
    return [
        QueryDef(
            name="q1_user_by_id",
            sql="SELECT * FROM Users WHERE idUser = %s;",
            params_factory=lambda seq, _: ((seq % args.max_user_id) + 1,),
        ),
        QueryDef(
            name="q2_all_users",
            sql="SELECT * FROM Users;",
            params_factory=lambda seq, _: tuple(),
        ),
        QueryDef(
            name="q3_user_by_login",
            sql="SELECT * FROM Users WHERE Login = %s;",
            params_factory=lambda seq, seed: (seed["logins"][seq % len(seed["logins"])],),
        ),
        QueryDef(
            name="q4_sellers_rating",
            sql="SELECT * FROM Sellers WHERE Rating >= 4 LIMIT 100;",
            params_factory=lambda seq, _: tuple(),
        ),
        QueryDef(
            name="q5_orders_offset",
            sql="SELECT * FROM Orders ORDER BY idOrder LIMIT 50 OFFSET 5000;",
            params_factory=lambda seq, _: tuple(),
        ),
        QueryDef(
            name="q6_products_by_seller",
            sql="SELECT * FROM Products WHERE Seller_id = %s;",
            params_factory=lambda seq, _: ((seq % args.max_seller_id) + 1,),
        ),
        QueryDef(
            name="q7_products_join_sellers",
            sql=(
                "SELECT p.*, s.Name "
                "FROM Products p "
                "JOIN Sellers s ON s.idSeller = p.Seller_id "
                "WHERE s.idSeller = %s;"
            ),
            params_factory=lambda seq, _: ((seq % args.max_seller_id) + 1,),
        ),
        QueryDef(
            name="q8_order_items_join_products",
            sql=(
                "SELECT oi.Product_id, oi.Quantity, p.Title, p.Price "
                "FROM Order_Items oi "
                "JOIN Products p ON p.idProduct = oi.Product_id "
                "WHERE oi.Order_id = %s;"
            ),
            params_factory=lambda seq, _: ((seq % args.max_order_id) + 1,),
        ),
        QueryDef(
            name="q9_orders_items_products_by_user",
            sql=(
                "SELECT o.idOrder, o.Create_Date, oi.Product_id, oi.Quantity, p.Title, p.Price "
                "FROM Orders o "
                "JOIN Order_Items oi ON oi.Order_id = o.idOrder "
                "JOIN Products p ON p.idProduct = oi.Product_id "
                "WHERE o.Users_id = %s;"
            ),
            params_factory=lambda seq, _: ((seq % args.max_user_id) + 1,),
        ),
        QueryDef(
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
        QueryDef(
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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="MySQL benchmark runner")
    p.add_argument("--memory-limit", default="2g", help="Container memory limit, e.g. 2g or 6g")
    p.add_argument("--cpus", type=float, default=1.0)
    p.add_argument("--concurrency", type=int, default=1, help="Concurrent users (threads), e.g. 1 or 10")
    p.add_argument("--warmup-requests", type=int, default=200)
    p.add_argument("--benchmark-requests", type=int, default=10000)
    p.add_argument("--stats-interval-sec", type=float, default=1.0)
    p.add_argument("--csv-folder", default="data_csv(third)")
    p.add_argument("--results-dir", default="results")

    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=3306)
    p.add_argument("--user", default="root")
    p.add_argument("--password", default="root")
    p.add_argument("--database", default="mydb")

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
        db_name="mysql",
        service_name="mysql",
        container_name="dbb-mysql",
        connect_factory=lambda: connect_mysql(args.host, args.port, args.user, args.password, args.database),
        execute_query=execute_mysql,
        close_connection=close_mysql,
        query_defs=build_queries(args),
        import_flags=["-SkipPostgres", "-SkipSqlite"],
        load_seed_data=load_seed_mysql,
        wait_ready=lambda: wait_mysql_ready(args.user, args.password),
    )
    runner.run(args)


if __name__ == "__main__":
    main()

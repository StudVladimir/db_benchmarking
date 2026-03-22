# Benchmark Scripts

Scripts:
- `benchmark_mysql.py`
- `benchmark_postgres.py`
- `benchmark_sqlite.py`

All scripts run the same 3 phases:
1. `load_data` (imports CSV into selected DB)
2. `warmup` (default 200 queries)
3. `benchmark` (default 10,000 queries)

Collected metrics:
- query latencies (per request)
- p50/p95/p99 latency
- ops/sec
- docker stats samples: CPU%, MEM%, MEM usage/limit, NET I/O, BLOCK I/O

Output CSV files are written into:
- `results/<db>/<timestamp>_mem-<limit>_u-<concurrency>/`

Files:
- `latencies.csv`
- `phase_summary.csv`
- `docker_stats_samples.csv`

## Dependencies

For MySQL:
```powershell
pip install pymysql
```

For PostgreSQL:
```powershell
pip install psycopg[binary]
```

SQLite script uses container `sqlite3` CLI and does not need extra Python packages.

## Example Scenarios

MySQL, 2GB RAM, 1 user:
```powershell
python .\benchmark_mysql.py --memory-limit 2g --concurrency 1
```

MySQL, 6GB RAM, 10 users:
```powershell
python .\benchmark_mysql.py --memory-limit 6g --concurrency 10
```

PostgreSQL, 2GB RAM, 1 user:
```powershell
python .\benchmark_postgres.py --memory-limit 2g --concurrency 1
```

PostgreSQL, 6GB RAM, 10 users:
```powershell
python .\benchmark_postgres.py --memory-limit 6g --concurrency 10
```

SQLite, 2GB RAM, 1 user:
```powershell
python .\benchmark_sqlite.py --memory-limit 2g --concurrency 1
```

SQLite, 6GB RAM, 10 users:
```powershell
python .\benchmark_sqlite.py --memory-limit 6g --concurrency 10
```

## Useful Flags

Skip load step:
```powershell
--skip-load-data
```

Do not truncate/reset data on import:
```powershell
--no-reset-data
```

Do not recreate container before run:
```powershell
--no-recreate-container
```

Change requests count:
```powershell
--warmup-requests 200 --benchmark-requests 10000
```

Change id ranges (for incrementing ids in parameterized queries):
```powershell
--max-user-id 10000 --max-seller-id 10000 --max-order-id 10000
```

from __future__ import annotations

import argparse
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

from autotrade.coreutils.config import ClickHouseInfo, DatabaseInfo, load_env
from autotrade.data.ricequant.init_rq_data import RQ_DATABASES


def _require_binary(binary_name: str) -> str:
    binary = shutil.which(binary_name)
    if binary is None:
        raise FileNotFoundError(
            f"Required binary not found in PATH: {binary_name}. "
            f"Please install it or add it to PATH."
        )
    return binary


def _run_command(command: list[str], *, stdout_path: Path | None = None) -> None:
    if stdout_path is None:
        subprocess.run(command, check=True)
        return

    stdout_path.parent.mkdir(parents=True, exist_ok=True)
    with stdout_path.open("wb") as f:
        subprocess.run(command, check=True, stdout=f)


def backup_mysql_database(*, mysqldump_bin: str, database: str, output_path: Path) -> None:
    command = [
        mysqldump_bin,
        f"--host={DatabaseInfo.host}",
        f"--port={DatabaseInfo.port}",
        f"--user={DatabaseInfo.user}",
        f"--password={DatabaseInfo.password}",
        "--single-transaction",
        "--quick",
        "--routines",
        "--triggers",
        "--events",
        "--set-gtid-purged=OFF",
        "--databases",
        database,
    ]
    _run_command(command, stdout_path=output_path)


def _query_clickhouse_text(*, clickhouse_client_bin: str, database: str | None, query: str) -> str:
    command = [
        clickhouse_client_bin,
        "--host",
        ClickHouseInfo.host,
        "--port",
        str(ClickHouseInfo.tcp_port),
        "--user",
        ClickHouseInfo.user,
        "--password",
        ClickHouseInfo.password,
        "--query",
        query,
    ]
    if database is not None:
        command.extend(["--database", database])

    result = subprocess.run(command, check=True, capture_output=True, text=True)
    return result.stdout


def backup_clickhouse_database(*, clickhouse_client_bin: str, database: str, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    ddl_dir = output_dir / "ddl"
    data_dir = output_dir / "data"
    ddl_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    database_ddl = _query_clickhouse_text(
        clickhouse_client_bin=clickhouse_client_bin,
        database=None,
        query=f"SHOW CREATE DATABASE `{database}`",
    )
    (ddl_dir / "_database.sql").write_text(database_ddl.strip() + ";\n", encoding="utf-8")

    tables_raw = _query_clickhouse_text(
        clickhouse_client_bin=clickhouse_client_bin,
        database=database,
        query="SHOW TABLES",
    )
    tables = [line.strip() for line in tables_raw.splitlines() if line.strip()]

    for table in tables:
        table_ddl = _query_clickhouse_text(
            clickhouse_client_bin=clickhouse_client_bin,
            database=database,
            query=f"SHOW CREATE TABLE `{table}`",
        )
        (ddl_dir / f"{table}.sql").write_text(table_ddl.strip() + ";\n", encoding="utf-8")

        data_path = data_dir / f"{table}.native"
        command = [
            clickhouse_client_bin,
            "--host",
            ClickHouseInfo.host,
            "--port",
            str(ClickHouseInfo.tcp_port),
            "--user",
            ClickHouseInfo.user,
            "--password",
            ClickHouseInfo.password,
            "--database",
            database,
            "--query",
            f"SELECT * FROM `{table}` FORMAT Native",
        ]
        _run_command(command, stdout_path=data_path)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backup RiceQuant MySQL and ClickHouse databases.")
    parser.add_argument(
        "--backup-root",
        default=r"D:\backup",
        help=r"Backup root directory. Default: D:\backup",
    )
    parser.add_argument(
        "--tag",
        default=None,
        help="Optional backup tag. Default uses current timestamp.",
    )
    return parser


def main() -> None:
    load_env()
    parser = build_arg_parser()
    args = parser.parse_args()

    mysqldump_bin = _require_binary("mysqldump")
    clickhouse_client_bin = _require_binary("clickhouse-client")

    backup_root = Path(args.backup_root)
    tag = args.tag or datetime.now().strftime("rq_backup_%Y%m%d_%H%M%S")
    backup_dir = backup_root / tag

    mysql_dir = backup_dir / "mysql"
    clickhouse_dir = backup_dir / "clickhouse"
    mysql_dir.mkdir(parents=True, exist_ok=True)
    clickhouse_dir.mkdir(parents=True, exist_ok=True)

    for database in sorted(RQ_DATABASES):
        backup_mysql_database(
            mysqldump_bin=mysqldump_bin,
            database=database,
            output_path=mysql_dir / f"{database}.sql",
        )
        backup_clickhouse_database(
            clickhouse_client_bin=clickhouse_client_bin,
            database=database,
            output_dir=clickhouse_dir / database,
        )

    print(f"Backup completed: {backup_dir}")


if __name__ == "__main__":
    main()

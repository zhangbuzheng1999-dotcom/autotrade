from __future__ import annotations

import argparse
import shutil
import subprocess
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


def _run_command(command: list[str], *, stdin_path: Path | None = None) -> None:
    if stdin_path is None:
        subprocess.run(command, check=True)
        return

    with stdin_path.open("rb") as f:
        subprocess.run(command, check=True, stdin=f)


def restore_mysql_database(*, mysql_bin: str, dump_path: Path) -> None:
    command = [
        mysql_bin,
        f"--host={DatabaseInfo.host}",
        f"--port={DatabaseInfo.port}",
        f"--user={DatabaseInfo.user}",
        f"--password={DatabaseInfo.password}",
    ]
    _run_command(command, stdin_path=dump_path)


def _clickhouse_command(*, clickhouse_client_bin: str, database: str | None, query: str) -> list[str]:
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
    return command


def restore_clickhouse_database(*, clickhouse_client_bin: str, database_dir: Path, drop_existing: bool) -> None:
    database = database_dir.name
    ddl_dir = database_dir / "ddl"
    data_dir = database_dir / "data"

    if drop_existing:
        _run_command(
            _clickhouse_command(
                clickhouse_client_bin=clickhouse_client_bin,
                database=None,
                query=f"DROP DATABASE IF EXISTS `{database}`",
            )
        )

    database_ddl = ddl_dir / "_database.sql"
    _run_command(
        _clickhouse_command(
            clickhouse_client_bin=clickhouse_client_bin,
            database=None,
            query=database_ddl.read_text(encoding="utf-8"),
        )
    )

    table_sql_files = sorted(p for p in ddl_dir.glob("*.sql") if p.name != "_database.sql")
    for table_sql in table_sql_files:
        _run_command(
            _clickhouse_command(
                clickhouse_client_bin=clickhouse_client_bin,
                database=None,
                query=table_sql.read_text(encoding="utf-8"),
            )
        )

    for native_file in sorted(data_dir.glob("*.native")):
        table = native_file.stem
        _run_command(
            _clickhouse_command(
                clickhouse_client_bin=clickhouse_client_bin,
                database=database,
                query=f"INSERT INTO `{table}` FORMAT Native",
            ),
            stdin_path=native_file,
        )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Restore RiceQuant MySQL and ClickHouse databases from backup.")
    parser.add_argument(
        "--backup-root",
        default=r"D:\backup",
        help=r"Backup root directory. Default: D:\backup",
    )
    parser.add_argument(
        "--tag",
        required=True,
        help="Backup tag directory name under backup root.",
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop existing ClickHouse databases before restore.",
    )
    return parser


def main() -> None:
    load_env()
    parser = build_arg_parser()
    args = parser.parse_args()

    mysql_bin = _require_binary("mysql")
    clickhouse_client_bin = _require_binary("clickhouse-client")

    backup_dir = Path(args.backup_root) / args.tag
    mysql_dir = backup_dir / "mysql"
    clickhouse_dir = backup_dir / "clickhouse"

    if not backup_dir.exists():
        raise FileNotFoundError(f"Backup directory not found: {backup_dir}")

    for database in sorted(RQ_DATABASES):
        mysql_dump = mysql_dir / f"{database}.sql"
        if mysql_dump.exists():
            restore_mysql_database(mysql_bin=mysql_bin, dump_path=mysql_dump)

    for database in sorted(RQ_DATABASES):
        database_dir = clickhouse_dir / database
        if database_dir.exists():
            restore_clickhouse_database(
                clickhouse_client_bin=clickhouse_client_bin,
                database_dir=database_dir,
                drop_existing=args.drop_existing,
            )

    print(f"Restore completed from: {backup_dir}")


if __name__ == "__main__":
    main()

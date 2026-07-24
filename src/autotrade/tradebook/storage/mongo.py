from __future__ import annotations

import datetime as dt

import pandas as pd
from pymongo import ASCENDING, MongoClient
from pymongo.errors import CollectionInvalid

from ...coreutils.config import MongoInfo

from .base import LedgerStorage
from .schema import (
    COLLECTION_COLUMNS,
    EQUITY_COLLECTION,
    INSTRUMENT_COLLECTION,
    POSITION_COLLECTION,
    TRADEBOOK_DB_NAME,
    TRADE_COLLECTION,
)


class MongoConn:
    def __init__(
        self,
        *,
        mongo_host: str,
        mongo_port: int,
        username: str,
        password: str,
        db_name: str = "",
        connect_timeout_ms: int = 500,
        try_times: int = 3,
    ):
        auth = ""
        if username:
            auth = username
            if password:
                auth += f":{password}"
            auth += "@"
        db_suffix = f"/{db_name}" if db_name else ""
        self.connect_info = f"mongodb://{auth}{mongo_host}:{mongo_port}{db_suffix}"
        self.db_obj = MongoClient(self.connect_info, connectTimeoutMS=connect_timeout_ms)
        self.is_connect = False
        self.try_times = try_times

    def reconnect(self) -> None:
        self.db_obj = MongoClient(self.connect_info, connectTimeoutMS=500)
        self.is_connect = False

    def get_data(self, db_name, col_name, match_cond, fields=None, is_df=True, id_flag=False):
        for idx in range(self.try_times):
            try:
                col = self.db_obj[db_name][col_name]
                projection = {"_id": 1 if id_flag else 0}
                if fields is not None:
                    projection.update({field: 1 for field in fields})
                cursor = col.find(match_cond, projection)

                if is_df:
                    data_df = pd.DataFrame(cursor).dropna(how="all")
                    data_df = data_df if fields is None else data_df.reindex(columns=fields)
                    return data_df
                return cursor
            except Exception:
                print("数据库 %s, %s 数据查询 失败, %s/3！" % (db_name, col_name, idx + 1))

        raise ConnectionError("数据库 get_data 失败")

    def update_data(
        self,
        db_name,
        col_name,
        update_data_list,
        match_key_list,
        update_one=True,
        upset=True,
        replace_document=False,
    ):
        for idx in range(self.try_times):
            try:
                col = self.db_obj[db_name][col_name]
                for data_dict in update_data_list:
                    match_cond = {match_key: data_dict[match_key] for match_key in match_key_list}
                    if replace_document:
                        if not update_one:
                            raise ValueError("replace_document=True 仅支持 update_one=True")
                        col.replace_one(match_cond, data_dict, upsert=upset)
                    elif update_one:
                        col.update_one(match_cond, {"$set": data_dict}, upsert=upset)
                    else:
                        col.update_many(match_cond, {"$set": data_dict}, upsert=upset)
                return
            except Exception:
                print("数据库 %s, %s 更新数据 失败, %s/3！" % (db_name, col_name, idx + 1))

        raise ConnectionError("数据库 update_data 失败")

    def delete_data(self, db_name, col_name, match_cond=None, print_flag=True):
        if match_cond is None:
            match_cond = {}

        for idx in range(self.try_times):
            try:
                col = self.db_obj[db_name][col_name]
                del_obj = col.delete_many(match_cond)
                if print_flag:
                    print(f"{del_obj.deleted_count} 个文档数据已删除")
                return
            except Exception:
                print("数据库 %s, %s 更新数据 失败, %s/3！" % (db_name, col_name, idx + 1))

        raise ConnectionError("数据库 delete_data 失败")

    def server_info(self):
        return self.db_obj.server_info()


def _build_conn(db_name: str = TRADEBOOK_DB_NAME, try_times: int = 3) -> MongoConn:
    return MongoConn(
        mongo_host=MongoInfo.host,
        mongo_port=MongoInfo.port,
        username=MongoInfo.user,
        password=MongoInfo.password,
        db_name=db_name,
        try_times=try_times,
    )


def _ensure_columns(df: pd.DataFrame | None, columns: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=columns)
    out = df.copy()
    for col in columns:
        if col not in out.columns:
            out[col] = pd.NA
    return out[columns].copy()


def _to_mongo_records(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    records: list[dict] = []
    for row in df.to_dict(orient="records"):
        clean: dict = {}
        for key, value in row.items():
            if pd.isna(value):
                clean[key] = None
            elif isinstance(value, pd.Timestamp):
                clean[key] = value.to_pydatetime()
            else:
                clean[key] = value
        records.append(clean)
    return records


def _with_legacy_aliases(collection_name: str, df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    out = df.copy()
    if "book_name" in out.columns and "strategy" not in out.columns:
        out["strategy"] = out["book_name"]

    if collection_name in {TRADE_COLLECTION, POSITION_COLLECTION}:
        if "order_book_id" in out.columns and "instrument_id" not in out.columns:
            out["instrument_id"] = out["order_book_id"]

    if collection_name == INSTRUMENT_COLLECTION:
        if "order_book_id" in out.columns and "instrument_id" not in out.columns:
            out["instrument_id"] = out["order_book_id"]

    if collection_name == EQUITY_COLLECTION:
        if "book_name" in out.columns and "strategy" not in out.columns:
            out["strategy"] = out["book_name"]
        if "fee_cum" in out.columns and "fee" not in out.columns:
            out["fee"] = pd.NA
        if "realized_pnl_cum" in out.columns and "realized_pnl" not in out.columns:
            out["realized_pnl"] = pd.NA
        if "daily_pnl" not in out.columns and "pnl_total" in out.columns:
            out["daily_pnl"] = out["pnl_total"]

    return out


def bootstrap_tradebook_collections(
    *,
    db_name: str = TRADEBOOK_DB_NAME,
    try_times: int = 3,
) -> list[str]:
    conn = _build_conn(db_name=db_name, try_times=try_times)
    db = conn.db_obj[db_name]

    for collection_name in [
        TRADE_COLLECTION,
        POSITION_COLLECTION,
        EQUITY_COLLECTION,
        INSTRUMENT_COLLECTION,
    ]:
        try:
            db.create_collection(collection_name)
        except CollectionInvalid:
            pass

    for collection_name, index_names in {
        TRADE_COLLECTION: ["acct_strategy_trade_date"],
        POSITION_COLLECTION: ["acct_strategy_position_date", "uniq_position_snapshot"],
        EQUITY_COLLECTION: ["uniq_equity_snapshot"],
        INSTRUMENT_COLLECTION: ["uniq_instrument_id"],
    }.items():
        for index_name in index_names:
            try:
                db[collection_name].drop_index(index_name)
            except Exception:
                pass

    db[TRADE_COLLECTION].create_index([("trade_id", ASCENDING)], unique=True, name="uniq_trade_id")
    db[TRADE_COLLECTION].create_index([("account", ASCENDING), ("book_name", ASCENDING), ("trade_date", ASCENDING)], name="acct_book_trade_date")

    db[POSITION_COLLECTION].create_index(
        [("date", ASCENDING), ("account", ASCENDING), ("book_name", ASCENDING), ("order_book_id", ASCENDING)],
        unique=True,
        name="uniq_position_snapshot_v2",
    )
    db[POSITION_COLLECTION].create_index(
        [("account", ASCENDING), ("book_name", ASCENDING), ("date", ASCENDING)],
        name="acct_book_position_date_v2",
    )

    db[EQUITY_COLLECTION].create_index(
        [("date", ASCENDING), ("account", ASCENDING), ("book_name", ASCENDING)],
        unique=True,
        name="uniq_equity_snapshot_v2",
    )

    db[INSTRUMENT_COLLECTION].create_index([("order_book_id", ASCENDING)], unique=True, name="uniq_order_book_id_v2")
    db[INSTRUMENT_COLLECTION].create_index([("symbol", ASCENDING), ("exchange", ASCENDING)], name="symbol_exchange")
    return sorted(db.list_collection_names())


class MongoLedgerStorage(LedgerStorage):
    def __init__(
        self,
        *,
        db_name: str = TRADEBOOK_DB_NAME,
        try_times: int = 3,
    ):
        self.db_name = db_name
        self.try_times = try_times
        self.conn = _build_conn(db_name=db_name, try_times=try_times)

    def _load_frame(
        self,
        collection_name: str,
        *,
        account: str | None = None,
        book_name: str | None = None,
        start_date: str | pd.Timestamp | None = None,
        end_date: str | pd.Timestamp | None = None,
        date_field: str,
    ) -> pd.DataFrame:
        match_cond: dict = {}
        if account is not None:
            match_cond["account"] = account
        if book_name is not None:
            match_cond["book_name"] = book_name
        if start_date is not None or end_date is not None:
            date_cond: dict = {}
            if start_date is not None:
                date_cond["$gte"] = pd.to_datetime(start_date).normalize().to_pydatetime()
            if end_date is not None:
                end = pd.to_datetime(end_date).normalize() + pd.Timedelta(days=1)
                date_cond["$lt"] = end.to_pydatetime()
            match_cond[date_field] = date_cond

        columns = COLLECTION_COLUMNS[collection_name]
        frame = self.conn.get_data(
            db_name=self.db_name,
            col_name=collection_name,
            match_cond=match_cond,
            fields=columns,
        )
        if frame.empty:
            return pd.DataFrame(columns=columns)
        if date_field in frame.columns:
            frame[date_field] = pd.to_datetime(frame[date_field], errors="coerce")
        return frame

    def _upsert_frame(
        self,
        collection_name: str,
        df: pd.DataFrame,
        *,
        match_keys: list[str],
        replace_document: bool = False,
    ) -> None:
        columns = COLLECTION_COLUMNS[collection_name]
        payload = _ensure_columns(df, columns)
        payload = _with_legacy_aliases(collection_name, payload)
        records = _to_mongo_records(payload)
        if not records:
            return
        self.conn.update_data(
            db_name=self.db_name,
            col_name=collection_name,
            update_data_list=records,
            match_key_list=match_keys,
            update_one=True,
            upset=True,
            replace_document=replace_document,
        )

    def save_trades(
        self,
        *,
        trade_df: pd.DataFrame,
        overwrite: bool = False,
    ) -> None:
        payload = _ensure_columns(trade_df, COLLECTION_COLUMNS[TRADE_COLLECTION])
        if payload.empty:
            return

        if overwrite:
            trade_ids = payload["trade_id"].dropna().astype(str).tolist()
            if trade_ids:
                self.conn.delete_data(
                    db_name=self.db_name,
                    col_name=TRADE_COLLECTION,
                    match_cond={"trade_id": {"$in": trade_ids}},
                    print_flag=False,
                )

        self._upsert_frame(
            TRADE_COLLECTION,
            payload,
            match_keys=["trade_id"],
            replace_document=True,
        )

    def load_trades(
        self,
        *,
        account: str | None = None,
        book_name: str | None = None,
        start_date: str | pd.Timestamp | None = None,
        end_date: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        return self._load_frame(
            TRADE_COLLECTION,
            account=account,
            book_name=book_name,
            start_date=start_date,
            end_date=end_date,
            date_field="trade_date",
        )

    def load_latest_positions(
        self,
        *,
        account: str,
        book_name: str,
        before_date: str | pd.Timestamp,
    ) -> pd.DataFrame:
        positions = self.load_positions(
            account=account,
            book_name=book_name,
            end_date=pd.to_datetime(before_date).normalize() - pd.Timedelta(days=1),
        )
        if positions.empty:
            return pd.DataFrame(columns=COLLECTION_COLUMNS[POSITION_COLLECTION][1:])
        positions["date"] = pd.to_datetime(positions["date"]).dt.normalize()
        last_date = positions["date"].max()
        return positions.loc[positions["date"] == last_date, COLLECTION_COLUMNS[POSITION_COLLECTION][1:]].reset_index(drop=True)

    def load_positions(
        self,
        *,
        account: str | None = None,
        book_name: str | None = None,
        start_date: str | pd.Timestamp | None = None,
        end_date: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        return self._load_frame(
            POSITION_COLLECTION,
            account=account,
            book_name=book_name,
            start_date=start_date,
            end_date=end_date,
            date_field="date",
        )

    def save_positions(
        self,
        *,
        date: str | pd.Timestamp,
        position_df: pd.DataFrame,
        overwrite: bool = True,
    ) -> None:
        snapshot_date = pd.to_datetime(date).normalize()
        payload = position_df.copy()
        payload.insert(0, "date", snapshot_date)
        if overwrite:
            accounts = payload["account"].dropna().astype(str).unique().tolist() if "account" in payload.columns else []
            books = payload["book_name"].dropna().astype(str).unique().tolist() if "book_name" in payload.columns else []
            match_cond = {"date": {"$gte": snapshot_date.to_pydatetime(), "$lt": (snapshot_date + pd.Timedelta(days=1)).to_pydatetime()}}
            if len(accounts) == 1:
                match_cond["account"] = accounts[0]
            if len(books) == 1:
                match_cond["book_name"] = books[0]
            self.conn.delete_data(
                db_name=self.db_name,
                col_name=POSITION_COLLECTION,
                match_cond=match_cond,
                print_flag=False,
            )
        self._upsert_frame(
            POSITION_COLLECTION,
            payload,
            match_keys=["date", "account", "book_name", "order_book_id"],
            replace_document=True,
        )

    def load_equity(
        self,
        *,
        account: str | None = None,
        book_name: str | None = None,
        start_date: str | pd.Timestamp | None = None,
        end_date: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        return self._load_frame(
            EQUITY_COLLECTION,
            account=account,
            book_name=book_name,
            start_date=start_date,
            end_date=end_date,
            date_field="date",
        )

    def save_equity(
        self,
        *,
        date: str | pd.Timestamp,
        equity_df: pd.DataFrame,
        overwrite: bool = True,
    ) -> None:
        snapshot_date = pd.to_datetime(date).normalize()
        payload = equity_df.copy()
        payload["date"] = snapshot_date
        if overwrite:
            accounts = payload["account"].dropna().astype(str).unique().tolist() if "account" in payload.columns else []
            books = payload["book_name"].dropna().astype(str).unique().tolist() if "book_name" in payload.columns else []
            match_cond = {"date": {"$gte": snapshot_date.to_pydatetime(), "$lt": (snapshot_date + pd.Timedelta(days=1)).to_pydatetime()}}
            if len(accounts) == 1:
                match_cond["account"] = accounts[0]
            if len(books) == 1:
                match_cond["book_name"] = books[0]
            self.conn.delete_data(
                db_name=self.db_name,
                col_name=EQUITY_COLLECTION,
                match_cond=match_cond,
                print_flag=False,
            )
        self._upsert_frame(
            EQUITY_COLLECTION,
            payload,
            match_keys=["date", "account", "book_name"],
            replace_document=True,
        )


def save_instruments(
    instrument_df: pd.DataFrame,
    *,
    db_name: str = TRADEBOOK_DB_NAME,
    try_times: int = 3,
) -> None:
    conn = _build_conn(db_name=db_name, try_times=try_times)
    payload = _ensure_columns(instrument_df, COLLECTION_COLUMNS[INSTRUMENT_COLLECTION])
    payload = _with_legacy_aliases(INSTRUMENT_COLLECTION, payload)
    records = _to_mongo_records(payload)
    if not records:
        return
    conn.update_data(
        db_name=db_name,
        col_name=INSTRUMENT_COLLECTION,
        update_data_list=records,
        match_key_list=["order_book_id"],
        update_one=True,
        upset=True,
        replace_document=True,
    )

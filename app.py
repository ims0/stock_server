from __future__ import annotations

from collections.abc import Callable
from datetime import date, timedelta
import json
import sqlite3
import time
from queue import Empty, Queue
from threading import Thread
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import akshare as ak
import os
import pandas as pd
import tushare as ts
import yfinance as yf
from flask import Flask, Response, jsonify, render_template, request, stream_with_context

from config import TRADE_FEES

app = Flask(__name__)

DB_PATH = os.path.join(app.root_path, "data", "stock_cache.db")
LOCAL_SYMBOLS_PATH = os.path.join(app.root_path, "data", "symbols.json")
DATA_SOURCES = {
    "auto": "自动",
    "akshare": "AkShare",
    "sina": "新浪财经",
    "tushare": "TuShare",
    "yfinance": "Yahoo Finance",
    "alphavantage": "Alpha Vantage",
}
TUSHARE_TOKEN_FALLBACK = "f028f82a7bd86c57e54607995b4ed38b7eb3894e357a882eb7a5f665"

_tushare_client: ts.pro.client.DataApi | None = None
_local_symbols_cache: dict[str, dict[str, str]] | None = None
_local_symbols_mtime: float | None = None


def _get_tushare_client() -> ts.pro.client.DataApi | None:
    global _tushare_client
    if _tushare_client is not None:
        return _tushare_client

    token = os.environ.get("TUSHARE_TOKEN", "").strip() or TUSHARE_TOKEN_FALLBACK
    if not token:
        return None

    ts.set_token(token)
    _tushare_client = ts.pro_api()
    return _tushare_client


def _has_tushare_token() -> bool:
    return bool(os.environ.get("TUSHARE_TOKEN", "").strip() or TUSHARE_TOKEN_FALLBACK)


def _ensure_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS kline_cache (
                market TEXT NOT NULL,
                code TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL NOT NULL,
                high REAL NOT NULL,
                low REAL NOT NULL,
                close REAL NOT NULL,
                volume REAL NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (market, code, date)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_kline_lookup ON kline_cache (market, code, date)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS symbol_cache (
                market TEXT NOT NULL,
                code TEXT NOT NULL,
                name TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (market, code)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_symbol_lookup ON symbol_cache (market, code)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS non_trading_cache (
                market TEXT NOT NULL,
                code TEXT NOT NULL,
                date TEXT NOT NULL,
                reason TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (market, code, date)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_non_trading_lookup ON non_trading_cache (market, code, date)"
        )


def _load_local_symbols() -> dict[str, dict[str, str]]:
    global _local_symbols_cache, _local_symbols_mtime

    try:
        stat_result = os.stat(LOCAL_SYMBOLS_PATH)
    except FileNotFoundError:
        return {"a": {}, "hk": {}}

    if _local_symbols_cache is not None and _local_symbols_mtime == stat_result.st_mtime:
        return _local_symbols_cache

    try:
        with open(LOCAL_SYMBOLS_PATH, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception:
        return _local_symbols_cache or {"a": {}, "hk": {}}

    normalized: dict[str, dict[str, str]] = {"a": {}, "hk": {}}
    for market in ("a", "hk"):
        mapping = payload.get(market, {}) if isinstance(payload, dict) else {}
        if not isinstance(mapping, dict):
            continue
        for raw_code, raw_name in mapping.items():
            name = str(raw_name).strip()
            if not name:
                continue
            code = str(raw_code).strip()
            if market == "hk" and code.isdigit():
                code = code.zfill(5)
            normalized[market][code] = name

    _local_symbols_cache = normalized
    _local_symbols_mtime = stat_result.st_mtime
    return normalized


def _get_local_symbol_name(market: str, code: str) -> str:
    symbols = _load_local_symbols()
    return symbols.get(market, {}).get(code, "")


def _search_local_symbol_by_name(query: str) -> tuple[str, str, str] | None:
    symbols = _load_local_symbols()
    for market, mapping in symbols.items():
        for code, name in mapping.items():
            if name == query or query in name:
                return market, code, name
    return None


def _load_cached_kline(market: str, code: str, start_date: str, end_date: str) -> pd.DataFrame:
    _ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """
            SELECT date, open, high, low, close, volume
            FROM kline_cache
            WHERE market = ? AND code = ? AND date BETWEEN ? AND ?
            ORDER BY date
            """,
            (market, code, start_date, end_date),
        ).fetchall()

    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    return pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"])


def _load_cached_kline_all(market: str, code: str) -> pd.DataFrame:
    _ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """
            SELECT date, open, high, low, close, volume
            FROM kline_cache
            WHERE market = ? AND code = ?
            ORDER BY date
            """,
            (market, code),
        ).fetchall()

    if not rows:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    return pd.DataFrame(rows, columns=["date", "open", "high", "low", "close", "volume"])


def _save_non_trading_dates(market: str, code: str, dates: set[str], reason: str = "休市") -> None:
    if not dates:
        return

    _ensure_db()
    now = date.today().isoformat()
    rows = [(market, code, day, reason, now) for day in sorted(dates)]
    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO non_trading_cache (market, code, date, reason, updated_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            rows,
        )


def _load_non_trading_dates(market: str, code: str, start_date: str, end_date: str) -> set[str]:
    _ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """
            SELECT date
            FROM non_trading_cache
            WHERE market = ? AND code = ? AND date BETWEEN ? AND ?
            ORDER BY date
            """,
            (market, code, start_date, end_date),
        ).fetchall()
    return {row[0] for row in rows}


def _load_non_trading_dates_all(market: str, code: str) -> set[str]:
    _ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """
            SELECT date
            FROM non_trading_cache
            WHERE market = ? AND code = ?
            ORDER BY date
            """,
            (market, code),
        ).fetchall()
    return {row[0] for row in rows}


def _all_dates(start_date: str, end_date: str) -> list[str]:
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    if end < start:
        return []
    return [d.strftime("%Y-%m-%d") for d in pd.date_range(start, end, freq="D")]


def _weekend_dates(start_date: str, end_date: str) -> set[str]:
    days = _all_dates(start_date, end_date)
    return {
        d
        for d in days
        if pd.to_datetime(d).weekday() >= 5
    }


def _infer_non_trading_dates(start_date: str, end_date: str, trading_frame: pd.DataFrame) -> set[str]:
    known_days = set(_all_dates(start_date, end_date))
    if not known_days:
        return set()

    # Do not mark future dates as non-trading.
    today = date.today().isoformat()
    known_days = {day for day in known_days if day <= today}
    trading_days = set()
    if not trading_frame.empty and "date" in trading_frame.columns:
        trading_days = {str(day) for day in trading_frame["date"].astype(str).tolist()}
    return known_days - trading_days


def _emit_progress(
    progress: list[str] | None,
    progress_callback: Callable[[str], None] | None,
    message: str,
) -> None:
    if progress is not None:
        progress.append(message)
    if progress_callback is not None:
        progress_callback(message)


def _build_output_frame(trading_frame: pd.DataFrame, closed_dates: set[str]) -> pd.DataFrame:
    trading = trading_frame.copy()
    if not trading.empty:
        trading["is_open"] = True
        trading["note"] = ""

    if not closed_dates:
        if trading.empty:
            return pd.DataFrame(
                columns=["date", "open", "high", "low", "close", "volume", "is_open", "note"]
            )
        return trading.sort_values("date")

    closed_frame = pd.DataFrame(
        {
            "date": sorted(closed_dates),
            "open": [None] * len(closed_dates),
            "high": [None] * len(closed_dates),
            "low": [None] * len(closed_dates),
            "close": [None] * len(closed_dates),
            "volume": [None] * len(closed_dates),
            "is_open": [False] * len(closed_dates),
            "note": ["休市"] * len(closed_dates),
        }
    )

    if trading.empty:
        return closed_frame

    merged = pd.concat([trading, closed_frame], ignore_index=True)
    merged = merged.drop_duplicates(subset=["date"], keep="first").sort_values("date")
    return merged


def _save_cached_kline(market: str, code: str, data: pd.DataFrame) -> None:
    if data.empty:
        return

    _ensure_db()
    now = date.today().isoformat()
    rows = [
        (
            market,
            code,
            row["date"],
            float(row["open"]),
            float(row["high"]),
            float(row["low"]),
            float(row["close"]),
            float(row["volume"]),
            now,
        )
        for _, row in data.iterrows()
    ]

    with sqlite3.connect(DB_PATH) as conn:
        conn.executemany(
            """
            INSERT OR REPLACE INTO kline_cache
                (market, code, date, open, high, low, close, volume, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def _merge_cached_and_fetched(cached: pd.DataFrame, fetched: pd.DataFrame) -> pd.DataFrame:
    if cached.empty:
        return fetched
    if fetched.empty:
        return cached
    merged = pd.concat([cached, fetched], ignore_index=True)
    merged = merged.drop_duplicates(subset=["date"], keep="last").sort_values("date")
    return merged


def _cache_summary() -> list[dict[str, str | int]]:
    _ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            """
            WITH all_days AS (
                SELECT market, code, date, updated_at, 1 AS is_trading, 0 AS is_non_trading
                FROM kline_cache
                UNION ALL
                SELECT market, code, date, updated_at, 0 AS is_trading, 1 AS is_non_trading
                FROM non_trading_cache
            )
            SELECT
                d.market,
                d.code,
                s.name,
                MIN(d.date) AS start_date,
                MAX(d.date) AS end_date,
                COUNT(*) AS rows,
                SUM(d.is_trading) AS trading_rows,
                SUM(d.is_non_trading) AS non_trading_rows,
                MAX(d.updated_at) AS updated_at
            FROM all_days AS d
            LEFT JOIN symbol_cache AS s
                ON d.market = s.market AND d.code = s.code
            GROUP BY d.market, d.code, s.name
            ORDER BY d.market, d.code
            """
        ).fetchall()

    result = []
    for (
        market,
        code,
        name,
        start_date,
        end_date,
        rows_count,
        trading_rows,
        non_trading_rows,
        updated_at,
    ) in rows:
        if not name:
            looked_up = _lookup_name_by_code(market, code)
            if looked_up:
                _upsert_symbol(market, code, looked_up)
                name = looked_up
        result.append(
            {
                "market": market,
                "market_label": "A股" if market == "a" else "港股",
                "code": code,
                "name": name or "",
                "start_date": start_date,
                "end_date": end_date,
                "rows": rows_count,
                "trading_rows": trading_rows or 0,
                "non_trading_rows": non_trading_rows or 0,
                "updated_at": updated_at,
            }
        )
    return result


def _upsert_symbol(market: str, code: str, name: str) -> None:
    if not name:
        return

    _ensure_db()
    now = date.today().isoformat()
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO symbol_cache (market, code, name, updated_at)
            VALUES (?, ?, ?, ?)
            """,
            (market, code, name, now),
        )


def _get_cached_symbol_name(market: str, code: str) -> str:
    _ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT name FROM symbol_cache WHERE market = ? AND code = ?",
            (market, code),
        ).fetchone()
    return row[0] if row else ""


def _fetch_with_cache(
    market: str,
    code: str,
    start_date: str,
    end_date: str,
    source: str,
    progress_callback: Callable[[str], None] | None = None,
) -> tuple[pd.DataFrame, str, list[str]]:
    progress: list[str] = []
    _emit_progress(progress, progress_callback, "步骤1/2：检查本地缓存")

    all_cached = _load_cached_kline_all(market, code)
    all_non_trading = _load_non_trading_dates_all(market, code)
    requested_cached = _filter_kline_range(all_cached, start_date, end_date)
    cached_non_trading = _load_non_trading_dates(market, code, start_date, end_date)
    request_weekends = _weekend_dates(start_date, end_date)
    if request_weekends:
        _save_non_trading_dates(market, code, request_weekends)
        all_non_trading = all_non_trading.union(request_weekends)
        cached_non_trading = cached_non_trading.union(request_weekends)

    all_known_dates = set(all_cached["date"].tolist()) | all_non_trading
    if all_known_dates:
        _emit_progress(
            progress,
            progress_callback,
            f"缓存已有 {len(all_known_dates)} 条，覆盖区间 {min(all_known_dates)} ~ {max(all_known_dates)}"
        )
    else:
        _emit_progress(progress, progress_callback, "缓存为空")

    known_dates = set(requested_cached["date"].tolist()) | cached_non_trading
    required_dates = set(_all_dates(start_date, end_date))
    if required_dates and required_dates.issubset(known_dates):
        _emit_progress(
            progress,
            progress_callback,
            "缓存已覆盖请求区间，返回缓存数据",
        )
        return _build_output_frame(requested_cached, cached_non_trading), "cache", progress

    if not all_cached.empty:
        cache_start = all_cached["date"].min()
        cache_end = all_cached["date"].max()
        if start_date >= cache_start and end_date <= cache_end and requested_cached.empty:
            _emit_progress(progress, progress_callback, "缓存已覆盖请求区间，直接返回缓存数据")
            return _build_output_frame(requested_cached, cached_non_trading), "cache", progress

    _emit_progress(progress, progress_callback, "步骤2/2：缓存不足，开始请求网络数据")
    fetched_parts: list[pd.DataFrame] = []
    used_source = ""

    if all_cached.empty:
        fetched, current_source = _fetch_kline_with_source(
            market, code, start_date, end_date, source, progress, progress_callback
        )
        if not fetched.empty:
            fetched_parts.append(fetched)
            used_source = current_source
            inferred_closed = _infer_non_trading_dates(start_date, end_date, fetched)
            if inferred_closed:
                _save_non_trading_dates(market, code, inferred_closed)
                _emit_progress(progress, progress_callback, f"补记休市日期 {len(inferred_closed)} 天")
    else:
        cache_start = all_cached["date"].min()
        cache_end = all_cached["date"].max()
        if start_date < cache_start:
            before_end = _shift_date(cache_start, -1)
            fetched_before, source_before = _fetch_kline_with_source(
                market, code, start_date, before_end, source, progress, progress_callback
            )
            if not fetched_before.empty:
                fetched_parts.append(fetched_before)
                used_source = used_source or source_before
                inferred_closed_before = _infer_non_trading_dates(start_date, before_end, fetched_before)
                if inferred_closed_before:
                    _save_non_trading_dates(market, code, inferred_closed_before)
                    _emit_progress(
                        progress,
                        progress_callback,
                        f"补记休市日期 {len(inferred_closed_before)} 天",
                    )
        if end_date > cache_end:
            after_start = _shift_date(cache_end, 1)
            fetched_after, source_after = _fetch_kline_with_source(
                market, code, after_start, end_date, source, progress, progress_callback
            )
            if not fetched_after.empty:
                fetched_parts.append(fetched_after)
                used_source = used_source or source_after
                inferred_closed_after = _infer_non_trading_dates(after_start, end_date, fetched_after)
                if inferred_closed_after:
                    _save_non_trading_dates(market, code, inferred_closed_after)
                    _emit_progress(
                        progress,
                        progress_callback,
                        f"补记休市日期 {len(inferred_closed_after)} 天",
                    )

    fetched = (
        pd.concat(fetched_parts, ignore_index=True) if fetched_parts else pd.DataFrame()
    )
    _save_cached_kline(market, code, fetched)

    merged = _merge_cached_and_fetched(all_cached, fetched)
    final_data = _filter_kline_range(merged, start_date, end_date)
    final_non_trading = _load_non_trading_dates(market, code, start_date, end_date)
    final_output = _build_output_frame(final_data, final_non_trading)

    if final_output.empty:
        _emit_progress(progress, progress_callback, "网络数据源未返回有效数据")
    else:
        _emit_progress(progress, progress_callback, f"查询完成，返回 {len(final_output)} 条数据")

    return final_output, used_source, progress


def _filter_kline_range(frame: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    if frame.empty:
        return frame

    data = frame.copy()
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    data = data[(data["date"] >= start) & (data["date"] <= end)]
    if data.empty:
        return data

    data["date"] = data["date"].dt.strftime("%Y-%m-%d")
    return data.sort_values("date")


def _shift_date(input_date: str, days: int) -> str:
    shifted = pd.to_datetime(input_date) + pd.Timedelta(days=days)
    return shifted.strftime("%Y-%m-%d")


def _to_yyyymmdd(input_date: str) -> str:
    return input_date.replace("-", "")


def _default_dates() -> tuple[str, str]:
    end = date.today()
    start = end - timedelta(days=183)
    return start.isoformat(), end.isoformat()


def _validate_code(market: str, code: str) -> str:
    cleaned = code.strip()
    if market == "a":
        if len(cleaned) != 6 or not cleaned.isdigit():
            raise ValueError("A股代码需为6位数字，例如 600519")
        return cleaned

    if market == "hk":
        if not cleaned.isdigit() or not (1 <= len(cleaned) <= 5):
            raise ValueError("港股代码需为1-5位数字，例如 700 或 00700")
        return cleaned.zfill(5)

    raise ValueError("不支持的市场类型")


def _infer_market_and_code(code: str) -> tuple[str, str]:
    cleaned = code.strip()
    if not cleaned.isdigit():
        raise ValueError("股票代码仅支持数字")

    if len(cleaned) == 6:
        return "a", cleaned

    if 1 <= len(cleaned) <= 5:
        return "hk", cleaned.zfill(5)

    raise ValueError("无法识别股票类型：A股需6位，港股需1-5位数字")


def _pick_column(frame: pd.DataFrame, candidates: list[str]) -> str:
    for col in candidates:
        if col in frame.columns:
            return col
    return ""


def _search_symbol_by_name(name: str) -> tuple[str, str, str] | None:
    query = name.strip()
    if not query:
        return None

    local_hit = _search_local_symbol_by_name(query)
    if local_hit:
        return local_hit

    try:
        a_data = ak.stock_zh_a_spot_em()
        result = _search_symbol_in_frame(a_data, "a", query)
        if result:
            return result
    except Exception:
        pass

    try:
        hk_data = ak.stock_hk_spot_em()
        result = _search_symbol_in_frame(hk_data, "hk", query)
        if result:
            return result
    except Exception:
        pass

    return None


def _search_symbol_in_frame(
    frame: pd.DataFrame, market: str, query: str
) -> tuple[str, str, str] | None:
    code_col = _pick_column(frame, ["代码", "code", "symbol", "股票代码"])
    name_col = _pick_column(frame, ["名称", "name", "股票名称"])
    if not code_col or not name_col:
        return None

    subset = frame[[code_col, name_col]].dropna()
    if subset.empty:
        return None

    subset[name_col] = subset[name_col].astype(str)
    exact = subset[subset[name_col] == query]
    if exact.empty:
        matches = subset[subset[name_col].str.contains(query, na=False)]
        if matches.empty:
            return None
        row = matches.iloc[0]
    else:
        row = exact.iloc[0]

    code = str(row[code_col]).strip()
    name = str(row[name_col]).strip()
    if market == "hk" and code.isdigit():
        code = code.zfill(5)
    return market, code, name


def _lookup_name_by_code(market: str, code: str) -> str:
    local_name = _get_local_symbol_name(market, code)
    if local_name:
        return local_name

    # Lightweight A-share code→name mapping (faster and more reliable)
    if market == "a":
        try:
            df = ak.stock_info_a_code_name()
            match = df[df["code"].astype(str) == code]
            if not match.empty:
                name = str(match.iloc[0]["name"]).strip()
                if name:
                    return name
        except Exception:
            pass

    # HK stock: use company profile endpoint which returns Chinese name
    if market == "hk":
        try:
            df = ak.stock_hk_company_profile_em(symbol=code)
            if not df.empty and "公司名称" in df.columns:
                name = str(df.iloc[0]["公司名称"]).strip()
                if name:
                    return name
        except Exception:
            pass

    try:
        if market == "a":
            data = ak.stock_zh_a_spot_em()
        else:
            data = ak.stock_hk_spot_em()
    except Exception:
        return _lookup_name_by_code_tushare(market, code)

    code_col = _pick_column(data, ["代码", "code", "symbol", "股票代码"])
    name_col = _pick_column(data, ["名称", "name", "股票名称"])
    if not code_col or not name_col:
        return ""

    data[code_col] = data[code_col].astype(str)
    match = data[data[code_col] == code]
    if match.empty:
        if market == "hk" and code.isdigit():
            match = data[data[code_col] == code.lstrip("0")]
    if match.empty:
        return ""

    name = str(match.iloc[0][name_col]).strip()
    return name or _lookup_name_by_code_tushare(market, code)


def _lookup_name_by_code_tushare(market: str, code: str) -> str:
    client = _get_tushare_client()
    if client is None:
        return ""

    try:
        if market == "a":
            data = client.stock_basic(fields="symbol,name")
            symbol_col = _pick_column(data, ["symbol", "code"]) or ""
        else:
            data = client.hk_basic(fields="ts_code,name")
            symbol_col = _pick_column(data, ["ts_code", "code", "symbol"]) or ""
    except Exception:
        return ""

    if data is None or data.empty or not symbol_col:
        return ""

    data[symbol_col] = data[symbol_col].astype(str)
    if market == "a":
        match = data[data[symbol_col] == code]
    else:
        hk_ts_code = f"{code}.HK"
        match = data[data[symbol_col].isin([code, hk_ts_code, code.lstrip("0")])]
    if match.empty:
        return ""

    name_col = _pick_column(data, ["name", "名称", "stock_name"]) or ""
    if not name_col:
        return ""

    return str(match.iloc[0][name_col]).strip()


def _resolve_input_symbol(input_value: str) -> tuple[str, str, str]:
    cleaned = input_value.strip()
    if not cleaned:
        raise ValueError("请输入股票代码")

    if cleaned.isdigit():
        market, code = _infer_market_and_code(cleaned)
        code = _validate_code(market, code)
        name = _get_cached_symbol_name(market, code)
        if not name:
            name = _lookup_name_by_code(market, code)
        return market, code, name

    result = _search_symbol_by_name(cleaned)
    if not result:
        raise ValueError("未找到匹配的股票名称")
    return result


def _fetch_kline(
    market: str, code: str, start_date: str, end_date: str, source: str
) -> pd.DataFrame:
    data, _ = _fetch_kline_with_source(
        market=market,
        code=code,
        start_date=start_date,
        end_date=end_date,
        source=source,
        progress=None,
    )
    return data


def _fetch_kline_with_source(
    market: str,
    code: str,
    start_date: str,
    end_date: str,
    source: str,
    progress: list[str] | None,
    progress_callback: Callable[[str], None] | None = None,
) -> tuple[pd.DataFrame, str]:
    start = _to_yyyymmdd(start_date)
    end = _to_yyyymmdd(end_date)

    if source != "auto":
        _emit_progress(progress, progress_callback, f"尝试数据源：{DATA_SOURCES.get(source, source)}")
        data = _fetch_kline_from_source(market, code, start_date, end_date, start, end, source)
        if data.empty:
            _emit_progress(progress, progress_callback, f"数据源 {DATA_SOURCES.get(source, source)} 返回空数据")
        else:
            _emit_progress(
                progress,
                progress_callback,
                f"数据源 {DATA_SOURCES.get(source, source)} 成功，返回 {len(data)} 条",
            )
        return data, source if not data.empty else ""

    candidates = ["akshare"]
    if market == "hk":
        candidates.extend(["tushare", "sina", "yfinance", "alphavantage"])
    else:
        candidates.extend(["sina", "tushare", "yfinance", "alphavantage"])

    for candidate in candidates:
        if candidate == "tushare" and not _has_tushare_token():
            _emit_progress(progress, progress_callback, "跳过 TuShare：未配置 Token")
            continue
        if candidate == "alphavantage" and not os.environ.get("ALPHAVANTAGE_API_KEY"):
            _emit_progress(progress, progress_callback, "跳过 Alpha Vantage：未配置 API Key")
            continue
        _emit_progress(progress, progress_callback, f"尝试数据源：{DATA_SOURCES.get(candidate, candidate)}")
        data = _fetch_kline_from_source(
            market, code, start_date, end_date, start, end, candidate
        )
        if data.empty:
            _emit_progress(progress, progress_callback, f"数据源 {DATA_SOURCES.get(candidate, candidate)} 无可用数据")
            continue
        if progress is not None:
            _emit_progress(
                progress,
                progress_callback,
                f"数据源 {DATA_SOURCES.get(candidate, candidate)} 成功，返回 {len(data)} 条",
            )
        return data, candidate

    return pd.DataFrame(), ""


def _fetch_kline_from_source(
    market: str,
    code: str,
    start_date: str,
    end_date: str,
    start: str,
    end: str,
    source: str,
) -> pd.DataFrame:

    if source == "akshare":
        data = _fetch_kline_akshare(market, code, start, end)
        return data if data is not None else pd.DataFrame()

    if source == "yfinance":
        data = _fetch_kline_yfinance(market, code, start_date, end_date)
        return data if data is not None else pd.DataFrame()

    if source == "alphavantage":
        data = _fetch_kline_alphavantage(market, code, start_date, end_date)
        return data if data is not None else pd.DataFrame()

    if source == "sina":
        data = _fetch_kline_sina(market, code, start_date, end_date)
        return data if data is not None else pd.DataFrame()

    if source == "tushare":
        data = _fetch_kline_tushare(market, code, start_date, end_date)
        return data if data is not None else pd.DataFrame()

    return pd.DataFrame()


def _sleep_backoff(attempt: int, base: float = 0.8, cap: float = 8.0) -> None:
    delay = min(cap, base * (2 ** attempt))
    time.sleep(delay)


def _retry_fetch(operation, retries: int = 2) -> pd.DataFrame | None:
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return operation()
        except Exception as exc:
            last_error = exc
            if attempt < retries:
                _sleep_backoff(attempt)
    return None


def _normalize_kline_frame(frame: pd.DataFrame) -> pd.DataFrame:
    required_cols = ["date", "open", "high", "low", "close", "volume"]
    if not all(col in frame.columns for col in required_cols):
        return pd.DataFrame()

    normalized = frame[required_cols].copy()
    normalized["date"] = pd.to_datetime(normalized["date"]).dt.strftime("%Y-%m-%d")
    for col in ["open", "high", "low", "close", "volume"]:
        normalized[col] = pd.to_numeric(normalized[col], errors="coerce")
    normalized = normalized.dropna().sort_values("date")
    return normalized


def _fetch_kline_akshare(
    market: str, code: str, start: str, end: str
) -> pd.DataFrame | None:
    def _call() -> pd.DataFrame:
        if market == "a":
            return ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start,
                end_date=end,
                adjust="",
            )
        return ak.stock_hk_hist(
            symbol=code,
            period="daily",
            start_date=start,
            end_date=end,
            adjust="",
        )

    data = _retry_fetch(_call, retries=2)
    if data is None or data.empty:
        return None

    rename_map = {
        "日期": "date",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
    }
    normalized = data.rename(columns=rename_map)
    normalized = _normalize_kline_frame(normalized)
    return normalized if not normalized.empty else None


def _fetch_kline_yfinance(
    market: str, code: str, start_date: str, end_date: str
) -> pd.DataFrame | None:
    yf_symbol = _to_yfinance_symbol(market, code)

    yf_data = None
    for attempt in range(4):
        try:
            yf_data = yf.download(
                yf_symbol,
                start=start_date,
                end=end_date,
                interval="1d",
                progress=False,
                auto_adjust=False,
            )
        except Exception:
            yf_data = None

        if yf_data is not None and not yf_data.empty:
            break
        if attempt < 3:
            _sleep_backoff(attempt)

    if yf_data is None or yf_data.empty:
        return None

    if isinstance(yf_data.columns, pd.MultiIndex):
        yf_data.columns = [item[0] for item in yf_data.columns]

    yf_data = yf_data.reset_index().rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    normalized = _normalize_kline_frame(yf_data)
    return normalized if not normalized.empty else None


def _fetch_kline_alphavantage(
    market: str, code: str, start_date: str, end_date: str
) -> pd.DataFrame | None:
    api_key = os.environ.get("ALPHAVANTAGE_API_KEY", "").strip()
    if not api_key:
        return None

    symbol = _to_yfinance_symbol(market, code)
    query = urlencode(
        {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": api_key,
            "datatype": "json",
        }
    )
    url = f"https://www.alphavantage.co/query?{query}"

    try:
        with urlopen(url, timeout=10) as response:
            payload = response.read().decode("utf-8")
    except Exception:
        return None

    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return None

    series = data.get("Time Series (Daily)")
    if not isinstance(series, dict):
        return None

    records = []
    for date_key, values in series.items():
        records.append(
            {
                "date": date_key,
                "open": values.get("1. open"),
                "high": values.get("2. high"),
                "low": values.get("3. low"),
                "close": values.get("4. close"),
                "volume": values.get("5. volume"),
            }
        )

    frame = pd.DataFrame(records)
    if frame.empty:
        return None

    frame = _normalize_kline_frame(frame)
    if frame.empty:
        return None

    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame[(frame["date"] >= start) & (frame["date"] <= end)]
    if frame.empty:
        return None
    frame["date"] = frame["date"].dt.strftime("%Y-%m-%d")
    return frame


def _fetch_kline_sina(
    market: str, code: str, start_date: str, end_date: str
) -> pd.DataFrame | None:
    symbol = _to_sina_symbol(market, code)
    if not symbol:
        return None

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Referer": "https://finance.sina.com.cn/",
    }

    params = urlencode({"symbol": symbol, "scale": "240", "ma": "no", "datalen": "1023"})
    if market == "hk":
        url = f"https://money.finance.sina.com.cn/stock/api/jsonp.php/var%20_{symbol}=/HK_MarketData.getKLineData?{params}"
    else:
        url = f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?{params}"

    try:
        request = Request(url, headers=headers)
        with urlopen(request, timeout=10) as response:
            payload = response.read().decode("utf-8")
    except Exception:
        return None

    if payload.startswith("var "):
        start = payload.find("=")
        payload = payload[start + 1 :].strip()
        if payload.endswith(";"):
            payload = payload[:-1]

    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return None

    if not isinstance(data, list):
        return None

    frame = pd.DataFrame(data)
    if frame.empty:
        return None

    frame = frame.rename(
        columns={
            "day": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        }
    )
    frame = _normalize_kline_frame(frame)
    if frame.empty:
        return None

    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame[(frame["date"] >= start) & (frame["date"] <= end)]
    if frame.empty:
        return None
    frame["date"] = frame["date"].dt.strftime("%Y-%m-%d")
    return frame


def _fetch_kline_tushare(
    market: str, code: str, start_date: str, end_date: str
) -> pd.DataFrame | None:
    client = _get_tushare_client()
    if client is None:
        return None

    ts_code = _to_tushare_symbol(market, code)
    if not ts_code:
        return None

    try:
        if market == "hk":
            data = client.hk_daily(
                ts_code=ts_code,
                start_date=_to_yyyymmdd(start_date),
                end_date=_to_yyyymmdd(end_date),
            )
        else:
            data = client.daily(
                ts_code=ts_code,
                start_date=_to_yyyymmdd(start_date),
                end_date=_to_yyyymmdd(end_date),
            )
    except Exception:
        return None

    if data is None or data.empty:
        return None

    data = data.rename(
        columns={
            "trade_date": "date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "vol": "volume",
        }
    )
    data = _normalize_kline_frame(data)
    return data if not data.empty else None


def _to_sina_symbol(market: str, code: str) -> str | None:
    if market == "hk":
        return f"hk{code}"

    if code.startswith(("6", "5", "9")):
        return f"sh{code}"
    if code.startswith(("0", "3")):
        return f"sz{code}"
    return None


def _to_tushare_symbol(market: str, code: str) -> str | None:
    if market == "hk":
        return f"{code}.HK"

    if code.startswith(("6", "5", "9")):
        return f"{code}.SH"
    if code.startswith(("0", "3")):
        return f"{code}.SZ"
    return None


def _to_yfinance_symbol(market: str, code: str) -> str:
    if market == "hk":
        return f"{int(code):04d}.HK"

    if code.startswith(("6", "5", "9")):
        return f"{code}.SS"
    return f"{code}.SZ"


@app.get("/api/sources/health")
def sources_health_api():
    code = request.args.get("code", "").strip()
    start_date = request.args.get("start_date", "").strip()
    end_date = request.args.get("end_date", "").strip()

    if not code:
        return jsonify({"error": "请输入股票代码"}), 400

    if not start_date or not end_date:
        end = date.today()
        start = end - timedelta(days=30)
        start_date = start_date or start.isoformat()
        end_date = end_date or end.isoformat()

    try:
        market, symbol, symbol_name = _resolve_input_symbol(code)
        if symbol_name:
            _upsert_symbol(market, symbol, symbol_name)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    results = []
    for source in ["akshare", "sina", "tushare", "yfinance", "alphavantage"]:
        if source == "tushare" and not _has_tushare_token():
            results.append(
                {
                    "source": source,
                    "label": DATA_SOURCES.get(source, source),
                    "ok": False,
                    "rows": 0,
                    "error": "未配置 Token",
                }
            )
            continue
        if source == "alphavantage" and not os.environ.get("ALPHAVANTAGE_API_KEY"):
            results.append(
                {
                    "source": source,
                    "label": DATA_SOURCES.get(source, source),
                    "ok": False,
                    "rows": 0,
                    "error": "未配置 API Key",
                }
            )
            continue

        try:
            data = _fetch_kline(
                market=market,
                code=symbol,
                start_date=start_date,
                end_date=end_date,
                source=source,
            )
            rows = 0 if data is None else len(data)
            results.append(
                {
                    "source": source,
                    "label": DATA_SOURCES.get(source, source),
                    "ok": rows > 0,
                    "rows": rows,
                    "error": "" if rows > 0 else "无数据",
                }
            )
        except Exception as exc:
            results.append(
                {
                    "source": source,
                    "label": DATA_SOURCES.get(source, source),
                    "ok": False,
                    "rows": 0,
                    "error": str(exc),
                }
            )

    return jsonify(
        {
            "code": symbol,
            "name": symbol_name,
            "market": market,
            "items": results,
        }
    )


@app.route("/")
def index():
    default_start, default_end = _default_dates()
    return render_template("index.html", default_start=default_start, default_end=default_end)


@app.route("/cache")
def cache_page():
    return render_template("cache.html")


def _execute_kline_query(
    code: str,
    start_date: str,
    end_date: str,
    source: str,
    progress_callback: Callable[[str], None] | None = None,
) -> tuple[dict[str, object], int]:
    if not code:
        return {"error": "请输入股票代码"}, 400

    if not start_date or not end_date:
        default_start, default_end = _default_dates()
        start_date = start_date or default_start
        end_date = end_date or default_end

    if source not in DATA_SOURCES:
        return {"error": "不支持的数据源"}, 400

    if source == "alphavantage" and not os.environ.get("ALPHAVANTAGE_API_KEY"):
        return {"error": "Alpha Vantage 未配置 API Key"}, 400

    if source == "tushare" and not _has_tushare_token():
        return {"error": "TuShare 未配置 Token"}, 400

    try:
        market, symbol, symbol_name = _resolve_input_symbol(code)
        if symbol_name:
            _upsert_symbol(market, symbol, symbol_name)
        data, used_source, progress = _fetch_with_cache(
            market=market,
            code=symbol,
            start_date=start_date,
            end_date=end_date,
            source=source,
            progress_callback=progress_callback,
        )
    except ValueError as exc:
        return {"error": str(exc)}, 400
    except Exception as exc:
        return {"error": f"数据获取失败: {exc}"}, 500

    if data.empty:
        return (
            {
                "error": "未查询到K线数据，请检查代码或时间范围",
                "progress": progress,
            },
            404,
        )

    final_source = used_source or source
    final_source_label = DATA_SOURCES.get(final_source, final_source)
    if final_source == "cache":
        final_source_label = "本地缓存"

    records = data.to_dict(orient="records")
    # NaN is not valid JSON; convert float NaN to None (→ JSON null)
    for row in records:
        for key, val in row.items():
            if isinstance(val, float) and val != val:
                row[key] = None

    return (
        {
            "market": market,
            "market_label": "A股" if market == "a" else "港股",
            "code": symbol,
            "name": symbol_name,
            "start_date": start_date,
            "end_date": end_date,
            "rows": len(data),
            "source": final_source,
            "source_label": final_source_label,
            "progress": progress,
            "data": records,
        },
        200,
    )


@app.get("/api/kline")
def kline_api():
    code = request.args.get("code", "").strip()
    start_date = request.args.get("start_date", "").strip()
    end_date = request.args.get("end_date", "").strip()
    source = request.args.get("source", "auto").strip().lower() or "auto"
    payload, status_code = _execute_kline_query(code, start_date, end_date, source)
    return jsonify(payload), status_code


@app.get("/api/kline/stream")
def kline_stream_api():
    code = request.args.get("code", "").strip()
    start_date = request.args.get("start_date", "").strip()
    end_date = request.args.get("end_date", "").strip()
    source = request.args.get("source", "auto").strip().lower() or "auto"

    queue: Queue[tuple[str, str]] = Queue()

    def push_progress(message: str) -> None:
        queue.put(("progress", message))

    def worker() -> None:
        try:
            payload, status_code = _execute_kline_query(
                code,
                start_date,
                end_date,
                source,
                progress_callback=push_progress,
            )
            result_str = json.dumps({"status": status_code, "payload": payload}, ensure_ascii=False)
        except Exception as exc:
            result_str = json.dumps({"status": 500, "payload": {"error": f"内部错误: {exc}"}}, ensure_ascii=False)
        queue.put(("result", result_str))

    Thread(target=worker, daemon=True).start()

    def event_stream():
        while True:
            try:
                event_name, raw_data = queue.get(timeout=0.5)
            except Empty:
                yield ": keepalive\n\n"
                continue

            if event_name == "progress":
                yield f"event: progress\ndata: {json.dumps({'message': raw_data}, ensure_ascii=False)}\n\n"
                continue

            if event_name == "result":
                yield f"event: result\ndata: {raw_data}\n\n"
                break

    response = Response(stream_with_context(event_stream()), mimetype="text/event-stream")
    response.headers["Cache-Control"] = "no-cache"
    response.headers["X-Accel-Buffering"] = "no"
    return response


@app.get("/api/fees")
def fees_api():
    """返回各市场交易费用配置，供前端展示及计算成本用。"""
    return jsonify(TRADE_FEES)


@app.get("/api/cache/summary")
def cache_summary_api():
    summary = _cache_summary()
    return jsonify({"items": summary, "count": len(summary)})


@app.delete("/api/cache")
def clear_cache():
    code = request.args.get("code", "").strip()
    _ensure_db()
    with sqlite3.connect(DB_PATH) as conn:
        if not code:
            deleted_kline = conn.execute("DELETE FROM kline_cache").rowcount
            deleted_non_trading = conn.execute("DELETE FROM non_trading_cache").rowcount
            deleted = deleted_kline + deleted_non_trading
            return jsonify({"deleted": deleted})

        try:
            market, symbol = _infer_market_and_code(code)
            symbol = _validate_code(market, symbol)
        except ValueError as exc:
            return jsonify({"error": str(exc)}), 400

        deleted_kline = conn.execute(
            "DELETE FROM kline_cache WHERE market = ? AND code = ?",
            (market, symbol),
        ).rowcount
        deleted_non_trading = conn.execute(
            "DELETE FROM non_trading_cache WHERE market = ? AND code = ?",
            (market, symbol),
        ).rowcount
        return jsonify(
            {
                "deleted": deleted_kline + deleted_non_trading,
                "market": market,
                "code": symbol,
            }
        )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))


def _to_yfinance_symbol(market: str, code: str) -> str:
    if market == "hk":
        return f"{int(code):04d}.HK"

    if code.startswith(("6", "5", "9")):
        return f"{code}.SS"
    return f"{code}.SZ"


from __future__ import annotations

from datetime import date, timedelta

import akshare as ak
import os
import pandas as pd
import yfinance as yf
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)


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


def _fetch_kline(market: str, code: str, start_date: str, end_date: str) -> pd.DataFrame:
    start = _to_yyyymmdd(start_date)
    end = _to_yyyymmdd(end_date)

    try:
        if market == "a":
            data = ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=start,
                end_date=end,
                adjust="",
            )
        else:
            data = ak.stock_hk_hist(
                symbol=code,
                period="daily",
                start_date=start,
                end_date=end,
                adjust="",
            )

        if data is not None and not data.empty:
            rename_map = {
                "日期": "date",
                "开盘": "open",
                "收盘": "close",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
            }
            normalized = data.rename(columns=rename_map)
            required_cols = ["date", "open", "high", "low", "close", "volume"]
            if all(col in normalized.columns for col in required_cols):
                normalized = normalized[required_cols].copy()
                normalized["date"] = pd.to_datetime(normalized["date"]).dt.strftime("%Y-%m-%d")
                for col in ["open", "high", "low", "close", "volume"]:
                    normalized[col] = pd.to_numeric(normalized[col], errors="coerce")
                normalized = normalized.dropna().sort_values("date")
                if not normalized.empty:
                    return normalized
    except Exception:
        pass

    yf_symbol = _to_yfinance_symbol(market, code)
    yf_data = yf.download(
        yf_symbol,
        start=start_date,
        end=end_date,
        interval="1d",
        progress=False,
        auto_adjust=False,
    )
    if yf_data is None or yf_data.empty:
        return pd.DataFrame()

    if isinstance(yf_data.columns, pd.MultiIndex):
        yf_data.columns = [item[0] for item in yf_data.columns]

    yf_data = yf_data.reset_index()
    yf_data = yf_data.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    required_cols = ["date", "open", "high", "low", "close", "volume"]
    if not all(col in yf_data.columns for col in required_cols):
        return pd.DataFrame()

    normalized = yf_data[required_cols].copy()
    normalized["date"] = pd.to_datetime(normalized["date"]).dt.strftime("%Y-%m-%d")
    for col in ["open", "high", "low", "close", "volume"]:
        normalized[col] = pd.to_numeric(normalized[col], errors="coerce")
    normalized = normalized.dropna().sort_values("date")

    return normalized


def _to_yfinance_symbol(market: str, code: str) -> str:
    if market == "hk":
        return f"{int(code):04d}.HK"

    if code.startswith(("6", "5", "9")):
        return f"{code}.SS"
    return f"{code}.SZ"


@app.route("/")
def index():
    default_start, default_end = _default_dates()
    return render_template("index.html", default_start=default_start, default_end=default_end)


@app.get("/api/kline")
def kline_api():
    market = request.args.get("market", "a").strip().lower()
    code = request.args.get("code", "").strip()
    start_date = request.args.get("start_date", "").strip()
    end_date = request.args.get("end_date", "").strip()

    if not code:
        return jsonify({"error": "请输入股票代码"}), 400

    if not start_date or not end_date:
        default_start, default_end = _default_dates()
        start_date = start_date or default_start
        end_date = end_date or default_end

    try:
        symbol = _validate_code(market, code)
        data = _fetch_kline(market=market, code=symbol, start_date=start_date, end_date=end_date)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": f"数据获取失败: {exc}"}), 500

    if data.empty:
        return jsonify({"error": "未查询到K线数据，请检查代码或时间范围"}), 404

    return jsonify(
        {
            "market": market,
            "code": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "rows": len(data),
            "data": data.to_dict(orient="records"),
        }
    )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))


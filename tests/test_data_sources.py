import json
import os
import sys
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import akshare as ak
import pandas as pd
import tushare as ts
import yfinance as yf


def fetch_akshare(market: str, code: str, start: str, end: str) -> int:
    if market == "a":
        df = ak.stock_zh_a_hist(
            symbol=code,
            period="daily",
            start_date=start,
            end_date=end,
            adjust="",
        )
    else:
        df = ak.stock_hk_hist(
            symbol=code,
            period="daily",
            start_date=start,
            end_date=end,
            adjust="",
        )
    return 0 if df is None else len(df)


def fetch_yfinance(market: str, code: str, start: str, end: str) -> int:
    if market == "hk":
        symbol = f"{int(code):04d}.HK"
    elif code.startswith(("6", "5", "9")):
        symbol = f"{code}.SS"
    else:
        symbol = f"{code}.SZ"

    df = yf.download(
        symbol,
        start=start,
        end=end,
        interval="1d",
        progress=False,
        auto_adjust=False,
    )
    if df is None or df.empty:
        return 0
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [item[0] for item in df.columns]
    return len(df)


def fetch_alphavantage(market: str, code: str, start: str, end: str) -> int:
    api_key = os.environ.get("ALPHAVANTAGE_API_KEY", "").strip()
    if not api_key:
        return 0

    if market == "hk":
        symbol = f"{int(code):04d}.HK"
    elif code.startswith(("6", "5", "9")):
        symbol = f"{code}.SS"
    else:
        symbol = f"{code}.SZ"

    query = urlencode(
        {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": api_key,
            "datatype": "json",
        }
    )
    url = f"https://www.alphavantage.co/query?{query}"

    with urlopen(url, timeout=10) as response:
        payload = response.read().decode("utf-8")

    data = json.loads(payload)
    series = data.get("Time Series (Daily)")
    if not isinstance(series, dict):
        return 0

    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end)
    count = 0
    for date_key in series:
        date_dt = pd.to_datetime(date_key)
        if start_dt <= date_dt <= end_dt:
            count += 1
    return count


def fetch_tushare(market: str, code: str, start: str, end: str) -> int:
    token = os.environ.get("TUSHARE_TOKEN", "").strip()
    if not token:
        return 0

    ts.set_token(token)
    client = ts.pro_api()

    if market == "hk":
        ts_code = f"{code}.HK"
        df = client.hk_daily(ts_code=ts_code, start_date=start.replace("-", ""), end_date=end.replace("-", ""))
    elif code.startswith(("6", "5", "9")):
        ts_code = f"{code}.SH"
        df = client.daily(ts_code=ts_code, start_date=start.replace("-", ""), end_date=end.replace("-", ""))
    else:
        ts_code = f"{code}.SZ"
        df = client.daily(ts_code=ts_code, start_date=start.replace("-", ""), end_date=end.replace("-", ""))

    return 0 if df is None else len(df)


def fetch_sina(market: str, code: str, start: str, end: str) -> int:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
        "Referer": "https://finance.sina.com.cn/",
    }

    if market == "hk":
        symbol = f"hk{code}"
        params = urlencode({"symbol": symbol, "scale": "240", "ma": "no", "datalen": "1023"})
        url = f"https://money.finance.sina.com.cn/stock/api/jsonp.php/var%20_{symbol}=/HK_MarketData.getKLineData?{params}"
    else:
        if code.startswith(("6", "5", "9")):
            symbol = f"sh{code}"
        elif code.startswith(("0", "3")):
            symbol = f"sz{code}"
        else:
            return 0
        params = urlencode({"symbol": symbol, "scale": "240", "ma": "no", "datalen": "1023"})
        url = f"https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?{params}"

    request = Request(url, headers=headers)
    with urlopen(request, timeout=10) as response:
        payload = response.read().decode("utf-8")

    if payload.startswith("var "):
        payload = payload[payload.find("=") + 1 :].strip().rstrip(";")

    data = json.loads(payload)
    if not isinstance(data, list):
        return 0

    frame = pd.DataFrame(data)
    if frame.empty or "day" not in frame.columns:
        return 0

    frame["day"] = pd.to_datetime(frame["day"])
    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end)
    frame = frame[(frame["day"] >= start_dt) & (frame["day"] <= end_dt)]
    return len(frame)


def main() -> int:
    market = os.environ.get("TEST_MARKET", "hk")
    code = os.environ.get("TEST_CODE", "09868")
    start = os.environ.get("TEST_START", "2025-01-01")
    end = os.environ.get("TEST_END", "2026-03-22")

    tests = [
        ("akshare", fetch_akshare),
        ("sina", fetch_sina),
        ("tushare", fetch_tushare),
        ("yfinance", fetch_yfinance),
        ("alphavantage", fetch_alphavantage),
    ]

    failed = 0
    for name, func in tests:
        try:
            rows = func(market, code, start, end)
            print(f"{name}: {rows} rows")
            if rows == 0:
                failed += 1
        except Exception as exc:
            failed += 1
            print(f"{name}: error {exc}")

    print("Done.")
    return 1 if failed == len(tests) else 0


if __name__ == "__main__":
    sys.exit(main())

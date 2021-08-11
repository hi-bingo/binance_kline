import time
import requests
from datetime import datetime
import numpy as np
import pandas as pd
from tqdm import tqdm

BASE_URL = "https://api.binance.com"
REQ_LIMIT = 1000
SUPPORT_INTERVAL = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"}


def get_support_symbols():
    res = []
    end_point = "/api/v3/exchangeInfo"
    resp = requests.get(BASE_URL + end_point)
    for symbol_info in resp.json()["symbols"]:
        if symbol_info["status"] == "TRADING":
            symbol = "{}/{}".format(symbol_info["baseAsset"].upper(), symbol_info["quoteAsset"].upper())
            res.append(symbol)
    return res


def get_klines(symbol, interval='1h', since=None, limit=1000, to=None):
    end_point = "/api/v3/klines"
    params = {
        'symbol': symbol,
        'interval': interval,
        'startTime': since * 1000,
        'limit': limit,
        'endTime': to * 1000
    }
    resp = requests.get(BASE_URL + end_point, params=params)
    return resp.json()


def download_full_klines(symbol, interval, start, end=None, save_to=None, req_interval=None, dimension="ohlcv"):
    if interval not in SUPPORT_INTERVAL:
        raise Exception("interval {} is not support!!!".format(interval))
    start_end_pairs = get_start_end_pairs(start, end, interval)
    klines = []
    for (start_ts, end_ts) in tqdm(start_end_pairs):
        tmp_kline = get_klines(symbol.replace("/", ""), interval, since=start_ts, limit=REQ_LIMIT, to=end_ts)
        klines.append(tmp_kline)
        if req_interval:
            time.sleep(req_interval)

    klines = np.concatenate(klines)
    data = []
    cols = ["open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume"]

    for i in range(len(klines)):
        tmp_kline = klines[i]
        data.append(tmp_kline[:-1])

    df = pd.DataFrame(np.array(data), columns=cols, dtype=np.float)
    for col in cols:
        if ("time" in col) or ("number" in col):
            df[col] = df[col].astype(np.int)

    if dimension == "ohlcv":
        df = df[cols[:6]]

    real_start = datetime.fromtimestamp(df["open_time"].values[0] / 1000).strftime("%Y-%m-%d")
    real_end = datetime.fromtimestamp(df["open_time"].values[-1] / 1000).strftime("%Y-%m-%d")

    if save_to:
        df.to_csv(save_to, index=False)
    else:
        df.to_csv("{}_{}_{}_{}.csv".format(symbol.replace("/", "-"), interval, real_start, real_end), index=False)


def get_start_end_pairs(start, end, interval):
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    if end is None:
        end_dt = datetime.now()
    else:
        end_dt = datetime.strptime(end, "%Y-%m-%d")
    start_dt_ts = int(time.mktime(start_dt.timetuple()))
    end_dt_ts = int(time.mktime(end_dt.timetuple()))

    ts_interval = interval_to_seconds(interval)

    res = []
    cur_start = cur_end = start_dt_ts
    while cur_end < end_dt_ts - ts_interval:
        cur_end = min(end_dt_ts, cur_start + (REQ_LIMIT - 1) * ts_interval)
        res.append((cur_start, cur_end))
        cur_start = cur_end + ts_interval
    return res


def interval_to_seconds(interval):
    seconds_per_unit = {"m": 60, "h": 60 * 60, "d": 24 * 60 * 60, "w": 7 * 24 * 60 * 60}
    return int(interval[:-1]) * seconds_per_unit[interval[-1]]


if __name__ == '__main__':
    symbols = get_support_symbols()
    download_full_klines(symbol="BTC/USDT", interval="15m", start="2021-07-01", end="2021-08-01",save_to="path_to_file")

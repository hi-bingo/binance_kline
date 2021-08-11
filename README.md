# 下载币安现货历史K线数据

## 用法

### 获取所有交易对
```
symbols = get_support_symbols()
```
### 下载数据至本地（默认下载ohlcv数据
```
download_full_klines(symbol="BTC/USDT", interval="15m", start="2021-07-01", end="2021-08-01",save_to="path_to_file")
```

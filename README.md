# BinanceDataCollector

一個以 .NET Worker Service 實作的 Binance 歷史資料收集器，會定期抓取市場資料並寫入 DuckDB。

目前專案支援：

- Spot
- USD-M Futures
- COIN-M Futures
- Funding Rate
- Premium Index Kline
- Index Price Kline
- Mark Price Kline

程式啟動後會依序執行以下流程：

1. 立即執行一次同步工作
2. 同步完成後封存 DuckDB 資料
3. 之後由 Hangfire 每日排程執行一次

## 環境需求

- .NET 10 SDK
- Docker / Docker Compose（如果要用容器執行）

## 設定方式

主要設定檔在 [BinanceDataCollector/appsettings.json](BinanceDataCollector/appsettings.json)。

需要先確認以下幾項：

### 1. 啟用的市場

```json
"Market": {
  "Spot": { "IsEnabled": false },
  "UsdFutures": { "IsEnabled": true },
  "CoinFutures": { "IsEnabled": false }
}
```

### 2. 流水線工作數

```json
"ProductionLine": {
  "GetLastTimeWorkerCount": 1,
  "InsertKlineWorkerCount": 2,
  "DeleteKlineWorkerCount": 2
}
```

### 3. 忽略幣種

```json
"IgnoneCoins": {
  "Spot": [],
  "UsdFutures": [],
  "CoinFutures": []
}
```

## 本機執行

在專案根目錄執行：

```bash
dotnet restore
dotnet run --project BinanceDataCollector/BinanceDataCollector.csproj
```

執行期間的日誌會輸出到 [BinanceDataCollector/Logs](BinanceDataCollector/Logs)。

## 使用 Docker Compose

專案根目錄已提供 [docker-compose.yml](docker-compose.yml)，可直接啟動 collector：

```bash
docker compose up -d
```

容器版預設會掛載：

- `./binance-data-collector/config/appsettings.json` 到容器內 `/app/appsettings.json`
- `./binance-data-collector/logs` 到容器內 `/app/Logs`
- `./binance-data-collector/data` 到容器內 `/app/Data`

啟動前請先建立對應資料夾與設定檔。

## 專案結構

- [BinanceDataCollector](BinanceDataCollector)：背景服務、排程、資料收集、DuckDB 寫入與封存流程
- [CollectorModels](CollectorModels)：DuckDB 儲存模型共用型別

## 備註

- 建議不要把正式環境的連線字串直接提交到版本庫

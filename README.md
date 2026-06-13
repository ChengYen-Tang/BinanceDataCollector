# BinanceDataCollector

一個以 .NET Worker Service 實作的 Binance 歷史資料收集器。

目前系統已完全改成 DuckDB 儲存架構，不再依賴 SQL Server、EF Core migration 或舊的 sharding table 設計。程式會定期抓取 Binance 歷史資料，直接寫入各 market / symbol 對應的 DuckDB 檔案，並在同步完成後封存整個資料目錄。

## 目前支援的資料

- Spot
- USD-M Futures
- COIN-M Futures
- Kline
- Premium Index Kline
- Index Price Kline
- Mark Price Kline
- Funding Rate
- Open Interest History
- Top Long/Short Position Ratio
- Top Long/Short Account Ratio
- Global Long/Short Account Ratio
- Taker Long/Short Ratio
- Basis
- AggTrades
- BookDepth

## 目前架構

### 儲存模型

- 一般資料型別以 `market/symbol.duckdb` 儲存
- `AggTrades` 與 `BookDepth` 也已改成每個 symbol 一個 DuckDB 檔案
- 不再有多個 thread 共同寫入同一個 DuckDB 檔案的設計

### 寫入流程

程式啟動後會：

1. 啟動 worker 與 production line
2. 立即觸發一次同步工作
3. 之後由 Hangfire 每日排程執行
4. 各 market 先 `PrepareAsync()` 抓 market info
5. 再 `DispatchAsync()` 發送各 symbol 的 gather / insert / delete 工作
6. 所有 pipeline 完成後，checkpoint 目前有修改過的 DuckDB
7. 將整個 `DataStorage` 封存成 `Data/BinanceDataCollector.7z`

### Checkpoint 與停止行為

- 一般 symbol 依正常流程完成時才做 checkpoint
- `AggTrades` / `BookDepth` 在 symbol scope 結束時 checkpoint
- 若收到停止訊號，系統會先要求 Hangfire job 停止，再等待當前工作安全收尾
- 封存流程會接收 cancellation，避免 shutdown 時無限制持續壓縮

### Temp 資料夾

下載中的 market data 暫存於：

```text
Tmp/BinanceMarketData/
```

- `AggTrades` / `BookDepth` 下載 zip 後會先解壓到 temp，再匯入 DuckDB
- symbol 正常完成後會清掉該 symbol 的 temp 目錄
- 若中途中止，temp 可能保留，供下次續跑或重新處理

## 目錄說明

### `DataStorage`

主要資料目錄，存放各資料型別的 DuckDB。

大致結構如下：

```text
DataStorage/
  Kline/
    Spot/
      BTCUSDT.duckdb
  PremiumIndexKline/
    UsdFutures/
      BTCUSDT.duckdb
  aggTrades/
    Spot/
      BTCUSDT.duckdb
  bookDepth/
    UsdFutures/
      BTCUSDT.duckdb
  SymbolInfo/
    SymbolInfo.duckdb
```

### `Data`

封存輸出目錄：

```text
Data/
  BinanceDataCollector.7z
  BinanceDataCollector.7z.sha256
```

### `Tmp`

下載與解壓過程的暫存目錄：

```text
Tmp/
  BinanceMarketData/
```

### `Logs`

執行中的應用程式日誌。

## 設定方式

主要設定檔在 [BinanceDataCollector/appsettings.json](BinanceDataCollector/appsettings.json)。

### 啟用的市場

```json
"Market": {
  "Spot": { "IsEnabled": false },
  "UsdFutures": { "IsEnabled": true },
  "CoinFutures": { "IsEnabled": false }
}
```

### 流水線 worker 數量

```json
"ProductionLine": {
  "GetLastTimeWorkerCount": 1,
  "InsertKlineWorkerCount": 2,
  "DeleteKlineWorkerCount": 2
}
```

目前 `ProductionLine` 主要控制的是 pipeline 內不同工作階段的併行數。實際 gather / insert 的行為還會受到各 controller 與 queue 流程影響。

### 忽略幣種

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

## 驗證工具

### BinanceDataIntegrityChecker

用來檢查 DuckDB 資料完整性。

目前會檢查：

- table 是否為空
- 是否存在無效列
- 數值欄位是否全為 0
- UTC 日期是否有缺洞

執行方式：

```bash
dotnet run --project BinanceDataIntegrityChecker/BinanceDataIntegrityChecker.csproj
```

其設定位於該專案自己的 `appsettings.json` / environment config，可控制：

- 根目錄
- 要檢查的 data type
- `MaxDegreeOfParallelism`
- `MaxMissingDaysToLog`

## 專案結構

- [BinanceDataCollector](BinanceDataCollector)：主收集器，包含 worker、Hangfire、collector controller、DuckDB 寫入與封存
- [CollectorModels](CollectorModels)：共用的 DuckDB / storage model 類型
- [BinanceDataIntegrityChecker](BinanceDataIntegrityChecker)：DuckDB 完整性檢查工具
- [BinanceDataCollector.Tests](BinanceDataCollector.Tests)：測試專案

## 已移除的舊架構

以下內容已不再是目前系統的一部分：

- SQL Server
- EF Core migration
- ShardingCore
- 舊的 SQL Server -> DuckDB migrator
- 共用單一 DuckDB 檔案的多 thread 寫入模式

## 備註

- `docker-compose.yml` 目前如果仍存在舊的 SQL Server 內容，代表它還沒跟著最新架構一起整理，不應視為現在的權威部署文件
- 建議不要把正式環境設定直接提交到版本庫

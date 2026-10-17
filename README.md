# BinanceDataCollector

一個以 .NET Worker Service 實作的 Binance 歷史資料收集器。

目前系統已完全改成 DuckDB 儲存架構，不再依賴 SQL Server、EF Core migration 或舊的 sharding table 設計。程式會定期抓取 Binance 歷史資料，直接寫入各 market / symbol 對應的 DuckDB 檔案。

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

### DuckDB 儲存設定

所有系列（包含 SymbolInfo）統一使用 `storage_compatibility_version=latest`，
讓目前 DuckDB 引擎可使用最新儲存編碼。區塊大小維持預設的 256 KiB。

| 資料系列 | Row Group Size |
|---|---:|
| AggTrades | 524288 |
| BookDepth | 262144 |
| Kline、其餘 API 系列與 SymbolInfo | 122880（DuckDB 預設） |

設定集中在共用連線入口；AggTrades / BookDepth 每次開啟寫入或 checkpoint 連線時，
會重新帶入 attachment-scoped row group 設定，不會為了設定參數提前建立檔案或目錄。
這兩類資料的同檔操作會序列化，不同 symbol 的檔案仍可並行處理。

`latest` 代表目前引擎支援的儲存格式，不是啟動時自動完整重建舊檔。
既有資料不會因套件升級而保證全部重新編碼；此次上線前會另行完整重建資料，
本次變更不加入自動重建或舊格式分支。新格式也不保證可由舊版 DuckDB 讀取。

### 時間主鍵編碼

為降低時間主鍵 ART 索引的空間成本，下列系列使用可逆的整數時間編碼。
模型、API 請求與儲存 helper 的續傳時間仍使用 Unix 毫秒；只有 DuckDB 實體主鍵改用新欄位名稱。

| 資料系列 | 實體 BIGINT PRIMARY KEY | 毫秒值還原 |
|---|---|---|
| Kline、PremiumIndexKline、IndexPriceKline、MarkPriceKline | `CloseTimeMinute` | `CloseTimeMinute * 60000 + 59999` |
| OpenInterestHistory、Basis、GlobalLongShortAccountRatio、TopLongShortAccountRatio、TopLongShortPositionRatio、TakerLongShortRatio | `Timestamp5Minute` | `Timestamp5Minute * 300000` |

- Kline 目前收集一分鐘 K 線；六種 API 系列目前請求五分鐘區間。所有 market 使用同一儲存合約。
- 每筆輸入都必須精確對齊上述時間網格；否則整個 batch 會拋出 `InvalidDataException`，不會截斷、四捨五入或先刪除既有資料。
- 真正的 PRIMARY KEY 與 NOT NULL 約束保留，重疊資料仍在同一 transaction 中先刪除再寫入；已接受的 batch 不會被取消訊號中斷。
- 續傳直接取編碼主鍵的 MAX 再還原毫秒。保留期限刪除採整數向上取整界線，正確處理未對齊的刪除時間，不逐列解碼後再篩選。
- 價格、數量等 DOUBLE 欄位不變。FundingRate 的時間可能未對齊，不做此編碼；AggTrades、BookDepth、SymbolInfo 也不改時間格式。
- 上線前須重建這些系列的舊資料。遇到舊毫秒欄位或缺少 PRIMARY KEY 的表會拒絕操作；舊 shared database 也不能直接複製成新格式，不會自動搬移、改寫或刪除它。

外部查詢工具必須使用新欄位名稱及單位，例如 Kline：

```sql
-- 主鍵範圍查詢請先把毫秒界線轉成分鐘編號，再對 CloseTimeMinute 篩選。
SELECT OpenTime, OpenPrice, HighPrice, LowPrice, ClosePrice,
       CloseTimeMinute * 60000 + 59999 AS CloseTime
FROM data
ORDER BY CloseTimeMinute;
```

`BinanceDataIntegrityChecker` 目前只檢查 AggTrades / BookDepth，其欄位與檢查規則不受影響；
`DataStorageManifest.json` 仍針對完整檔案計算 SHA-256，不依賴表欄位名稱。
未全面套用 DECIMAL、小 block、強制 codec 或 value-first 排序，因實驗結果並非各系列皆有利。

### 寫入流程

程式啟動後會：

1. 啟動 worker 與 production line
2. 立即觸發一次同步工作
3. 之後由 Hangfire 每日排程執行
4. 各 market 先 `PrepareAsync()` 抓 market info
5. 再 `DispatchAsync()` 發送各 symbol 的 gather / insert / delete 工作
6. 所有 pipeline 完成後，checkpoint 目前有修改過的 DuckDB
7. 更新 `DataStorage/DataStorageManifest.json`，記錄所有 DuckDB 檔案與 SHA-256

### Checkpoint 與停止行為

- 一般 symbol 依正常流程完成時才做 checkpoint
- `AggTrades` / `BookDepth` 在 symbol scope 結束時 checkpoint
- 若收到停止訊號，系統會先要求 Hangfire job 停止，再等待當前工作安全收尾

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
  DataStorageManifest.json
```

`DataStorageManifest.json` 會在每次 pipeline 正常完成後更新，記錄每個 `*.duckdb` 的相對路徑與 SHA-256。

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

### DataStorage 完整性檢查

```json
"DataStorage": {
  "ValidateIntegrityOnStartup": false
}
```

設為 `true` 時，每次 Hangfire task 開始、寫入資料前會讀取 `DataStorageManifest.json`，逐一確認 manifest 記錄的 DuckDB 檔案存在且 SHA-256 相符；缺少 manifest、缺檔或 hash 不一致都會丟例外，使 task 失敗。新建立的 DuckDB 檔案會在本次 task 正常完成後寫入新的 manifest。首次啟用時，應先以 `false` 完成一次同步以建立 manifest。

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

- [BinanceDataCollector](BinanceDataCollector)：主收集器，包含 worker、Hangfire、collector controller 與 DuckDB 寫入
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

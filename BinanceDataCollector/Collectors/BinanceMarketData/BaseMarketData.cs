using System.Globalization;
using System.Net;
using System.Security.Cryptography;
using System.Xml.Linq;

namespace BinanceDataCollector.Collectors.BinanceMarketData;

internal abstract class BaseMarketData
{
    private const string BucketListingBaseUrl = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision";
    private static readonly XNamespace S3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/";
    protected static readonly HttpClient client = new()
    {
        BaseAddress = new Uri("https://data.binance.vision/"),
        Timeout = TimeSpan.FromMinutes(30)
    };

    protected abstract string MarketPathSegment { get; }
    protected abstract string MarketDataRemotePathSegment { get; }
    public abstract Task<Result<MarketDataDownloadBatch>> DownloadAggTradesAsync(string symbol, DateTime startTime, string tempSymbolPath, CancellationToken ct = default);

    protected async Task<Result<MarketDataDownloadBatch>> DownloadAsync(string dataType, string symbol, DateTime startTime, string tempSymbolPath, CancellationToken ct = default)
    {
        try
        {
            DateTime todayUtc = DateTime.UtcNow.Date;
            DateTime lastDailyDate = todayUtc.AddDays(-1);
            if (startTime.Date > lastDailyDate)
                return Result.Ok(CreateEmptyBatch(dataType, symbol));

            DateTime currentMonthStart = new(todayUtc.Year, todayUtc.Month, 1);
            DateTime startMonth = new(startTime.Year, startTime.Month, 1);
            DateTime lastCompletedMonth = currentMonthStart.AddMonths(-1);

            List<MarketDataDownloadFile> files = [];
            if (startMonth <= lastCompletedMonth)
            {
                IReadOnlyList<string> monthlyFileNames = await GetAvailableMonthlyFileNamesAsync(dataType, symbol, startMonth, lastCompletedMonth, ct);
                foreach (string fileName in monthlyFileNames)
                {
                    MarketDataDownloadFile file = CreateDownloadFile(
                        dataType,
                        symbol,
                        "monthly",
                        fileName,
                        Path.Combine(tempSymbolPath, "Monthly"));
                    if (await EnsureDownloadFileAsync(file, ct))
                        files.Add(file);
                }
            }

            DateTime dailyStartDate = startTime.Date > currentMonthStart ? startTime.Date : currentMonthStart;
            if (dailyStartDate <= lastDailyDate)
            {
                IReadOnlyList<string> dailyFileNames = await GetAvailableDailyFileNamesAsync(dataType, symbol, dailyStartDate, lastDailyDate, ct);
                foreach (string fileName in dailyFileNames)
                {
                    MarketDataDownloadFile file = CreateDownloadFile(
                        dataType,
                        symbol,
                        "daily",
                        fileName,
                        Path.Combine(tempSymbolPath, "Daily"));
                    if (await EnsureDownloadFileAsync(file, ct))
                        files.Add(file);
                }
            }

            return Result.Ok(new MarketDataDownloadBatch
            {
                MarketPathSegment = MarketPathSegment,
                DataType = dataType,
                Symbol = symbol,
                Files = files,
            });
        }
        catch (Exception ex)
        {
            return Result.Fail(ex.Message);
        }
    }

    private MarketDataDownloadBatch CreateEmptyBatch(string dataType, string symbol)
        => new()
        {
            MarketPathSegment = MarketPathSegment,
            DataType = dataType,
            Symbol = symbol,
            Files = [],
        };

    private MarketDataDownloadFile CreateDownloadFile(string dataType, string symbol, string remotePeriod, string fileName, string tempDirectory)
    {
        return new MarketDataDownloadFile
        {
            DataType = dataType,
            Symbol = symbol,
            Period = remotePeriod,
            FileName = fileName,
            RelativeZipPath = BuildRelativePath(dataType, symbol, remotePeriod, fileName),
            RelativeChecksumPath = BuildRelativePath(dataType, symbol, remotePeriod, fileName) + ".CHECKSUM",
            TempZipPath = Path.Combine(tempDirectory, fileName),
            TempChecksumPath = Path.Combine(tempDirectory, fileName + ".CHECKSUM"),
        };
    }

    private static bool HasCompleteFile(string directoryPath, string fileName)
        => File.Exists(Path.Combine(directoryPath, fileName))
        && File.Exists(Path.Combine(directoryPath, fileName + ".CHECKSUM"));

    private async Task<bool> EnsureDownloadFileAsync(MarketDataDownloadFile file, CancellationToken ct)
    {
        if (HasCompleteFile(Path.GetDirectoryName(file.TempZipPath)!, file.FileName))
        {
            if (await TryValidateChecksumAsync(file, ct))
                return true;

            DeleteFileIfExists(file.TempZipPath);
            DeleteFileIfExists(file.TempChecksumPath);
        }

        bool downloaded = await DownloadFilePairAsync(file, ct);
        if (!downloaded)
            return false;

        await ValidateChecksumAsync(file, ct);
        return true;
    }

    private async Task<bool> DownloadFilePairAsync(MarketDataDownloadFile file, CancellationToken ct)
    {
        bool hasZip = await DownloadFileAsync(file.RelativeZipPath, file.TempZipPath, ct);
        if (!hasZip)
            return false;

        bool hasChecksum = await DownloadFileAsync(file.RelativeChecksumPath, file.TempChecksumPath, ct);
        if (!hasChecksum)
            throw new InvalidDataException($"Checksum file not found. Path: {file.RelativeChecksumPath}");

        return true;
    }

    private static async Task ValidateChecksumAsync(MarketDataDownloadFile file, CancellationToken ct)
    {
        string checksumContent = await File.ReadAllTextAsync(file.TempChecksumPath, ct);
        string? expectedHash = checksumContent
            .Split([' ', '\t', '\r', '\n'], StringSplitOptions.RemoveEmptyEntries)
            .FirstOrDefault();

        if (string.IsNullOrWhiteSpace(expectedHash))
            throw new InvalidDataException($"Invalid checksum file: {file.TempChecksumPath}");

        await using FileStream stream = File.OpenRead(file.TempZipPath);
        byte[] actualHash = await SHA256.HashDataAsync(stream, ct);
        string actualHashText = Convert.ToHexString(actualHash);
        if (!string.Equals(actualHashText, expectedHash, StringComparison.OrdinalIgnoreCase))
            throw new InvalidDataException($"Checksum mismatch. File: {file.TempZipPath}");
    }

    private static async Task<bool> TryValidateChecksumAsync(MarketDataDownloadFile file, CancellationToken ct)
    {
        try
        {
            await ValidateChecksumAsync(file, ct);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private async Task<IReadOnlyList<string>> GetAvailableMonthlyFileNamesAsync(string dataType, string symbol, DateTime startMonth, DateTime lastCompletedMonth, CancellationToken ct)
    {
        string prefix = BuildPrefix(dataType, symbol, "monthly");
        string marker = prefix;
        IReadOnlyList<string> keys = await ListKeysAsync(prefix, marker, ct);
        return keys
            .Select(static key => Path.GetFileName(key))
            .Where(static fileName => fileName is not null)
            .Cast<string>()
            .Where(fileName => TryParsePeriod(fileName, symbol, dataType, "yyyy-MM", out DateTime fileMonth)
                && fileMonth >= startMonth
                && fileMonth <= lastCompletedMonth)
            .OrderBy(fileName => fileName, StringComparer.Ordinal)
            .ToArray();
    }

    private async Task<IReadOnlyList<string>> GetAvailableDailyFileNamesAsync(string dataType, string symbol, DateTime startDate, DateTime lastDailyDate, CancellationToken ct)
    {
        string prefix = BuildPrefix(dataType, symbol, "daily");
        string marker = prefix;
        IReadOnlyList<string> keys = await ListKeysAsync(prefix, marker, ct);
        return keys
            .Select(static key => Path.GetFileName(key))
            .Where(static fileName => fileName is not null)
            .Cast<string>()
            .Where(fileName => TryParsePeriod(fileName, symbol, dataType, "yyyy-MM-dd", out DateTime fileDate)
                && fileDate >= startDate
                && fileDate <= lastDailyDate)
            .OrderBy(fileName => fileName, StringComparer.Ordinal)
            .ToArray();
    }

    private static async Task<bool> DownloadFileAsync(string relativePath, string destinationPath, CancellationToken ct)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(destinationPath)!);
        if (File.Exists(destinationPath))
            File.Delete(destinationPath);

        using HttpResponseMessage response = await client.GetAsync(relativePath, HttpCompletionOption.ResponseHeadersRead, ct);
        if (response.StatusCode == HttpStatusCode.NotFound)
            return false;
        response.EnsureSuccessStatusCode();

        await using FileStream stream = File.Create(destinationPath);
        await response.Content.CopyToAsync(stream, ct);
        return true;
    }

    private static async Task<IReadOnlyList<string>> ListKeysAsync(string prefix, string marker, CancellationToken ct)
    {
        List<string> keys = [];
        string? currentMarker = marker;

        while (true)
        {
            string url = $"{BucketListingBaseUrl}?prefix={Uri.EscapeDataString(prefix)}&marker={Uri.EscapeDataString(currentMarker)}&max-keys=1000";
            using HttpResponseMessage response = await client.GetAsync(url, ct);
            response.EnsureSuccessStatusCode();

            string content = await response.Content.ReadAsStringAsync(ct);
            XDocument document = XDocument.Parse(content);
            List<string> pageKeys = document.Root?
                .Elements(S3Namespace + "Contents")
                .Select(item => item.Element(S3Namespace + "Key")?.Value)
                .Where(static key => !string.IsNullOrWhiteSpace(key) && !key.EndsWith(".CHECKSUM", StringComparison.OrdinalIgnoreCase))
                .Cast<string>()
                .ToList() ?? [];

            keys.AddRange(pageKeys);

            bool isTruncated = string.Equals(document.Root?.Element(S3Namespace + "IsTruncated")?.Value, "true", StringComparison.OrdinalIgnoreCase);
            if (!isTruncated || pageKeys.Count == 0)
                break;

            currentMarker = document.Root?.Element(S3Namespace + "NextMarker")?.Value ?? pageKeys[^1];
        }

        return keys;
    }

    private string BuildPrefix(string dataType, string symbol, string period)
        => $"data/{MarketDataRemotePathSegment}/{period}/{dataType}/{symbol}/";

    private string BuildRelativePath(string dataType, string symbol, string period, string fileName)
        => $"data/{MarketDataRemotePathSegment}/{period}/{dataType}/{symbol}/{fileName}";

    private static bool TryParsePeriod(string? fileName, string symbol, string dataType, string format, out DateTime value)
    {
        value = default;
        if (string.IsNullOrWhiteSpace(fileName) || !fileName.EndsWith(".zip", StringComparison.OrdinalIgnoreCase))
            return false;

        string prefix = $"{symbol}-{dataType}-";
        if (!fileName.StartsWith(prefix, StringComparison.Ordinal))
            return false;

        string datePart = fileName[prefix.Length..^4];
        return DateTime.TryParseExact(datePart, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out value);
    }

    private static void DeleteFileIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}

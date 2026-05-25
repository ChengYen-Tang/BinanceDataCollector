using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;

namespace BinanceDataCollector;

internal static class MarketDataArchiveHelper
{
    public const string ArchiveFileName = "BinanceMarketData.zip";
    private const string HashFileName = ArchiveFileName + ".sha256";
    private static readonly string BasePath = AppDomain.CurrentDomain.BaseDirectory;
    private static readonly string MarketDataPath = Path.Combine(BasePath, "BinanceMarketData");
    private static readonly string DataPath = Path.Combine(BasePath, "Data");
    private static readonly string ArchivePath = Path.Combine(DataPath, ArchiveFileName);
    private static readonly string HashPath = Path.Combine(DataPath, HashFileName);

    public static async Task FinalizeArchiveAsync(CancellationToken ct = default)
    {
        Directory.CreateDirectory(DataPath);
        Directory.CreateDirectory(MarketDataPath);
        DeleteFileIfExists(ArchivePath);
        DeleteFileIfExists(HashPath);

        ZipFile.CreateFromDirectory(MarketDataPath, ArchivePath, CompressionLevel.Optimal, includeBaseDirectory: false);

        string hashText = await ComputeSha256Async(ArchivePath, ct);
        await File.WriteAllTextAsync(HashPath, $"{hashText} *{ArchiveFileName}", Encoding.ASCII, ct);
    }

    private static void DeleteFileIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private static async Task<string> ComputeSha256Async(string path, CancellationToken ct)
    {
        await using FileStream stream = File.OpenRead(path);
        byte[] hash = await SHA256.HashDataAsync(stream, ct);
        return Convert.ToHexString(hash);
    }
}

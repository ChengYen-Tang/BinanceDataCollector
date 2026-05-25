using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;

namespace BinanceDataCollector;

internal static class CsvExportArchiveHelper
{
    public const string ArchiveFileName = "BinanceDataCollector.zip";
    private const string HashFileName = ArchiveFileName + ".sha256";
    private static readonly string BasePath = AppDomain.CurrentDomain.BaseDirectory;

    public static string DataPath { get; } = Path.Combine(BasePath, "Data");
    public static string TmpPath { get; } = Path.Combine(BasePath, "Tmp");
    public static string WorkRootPath { get; } = Path.Combine(TmpPath, "CsvExport");
    public static string ArchivePath { get; } = Path.Combine(DataPath, ArchiveFileName);
    public static string HashPath { get; } = Path.Combine(DataPath, HashFileName);

    public static void PrepareWorkRoot()
    {
        if (Directory.Exists(WorkRootPath))
            Directory.Delete(WorkRootPath, true);

        Directory.CreateDirectory(WorkRootPath);
    }

    public static void CleanupWorkRoot()
    {
        if (Directory.Exists(WorkRootPath))
            Directory.Delete(WorkRootPath, true);

        if (Directory.Exists(TmpPath))
            Directory.Delete(TmpPath, true);
    }

    public static async Task FinalizeArchiveAsync(CancellationToken ct = default)
    {
        Directory.CreateDirectory(DataPath);
        DeleteFileIfExists(ArchivePath);
        DeleteFileIfExists(HashPath);

        ZipFile.CreateFromDirectory(WorkRootPath, ArchivePath, CompressionLevel.Optimal, includeBaseDirectory: false);

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

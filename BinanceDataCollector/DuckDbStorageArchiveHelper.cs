using System.Security.Cryptography;
using System.Text;
using SharpSevenZip;

namespace BinanceDataCollector;

internal static class DuckDbStorageArchiveHelper
{
    public const string ArchiveFileName = "BinanceDataCollector.7z";
    private const string HashFileName = ArchiveFileName + ".sha256";
    private static readonly string BasePath = AppDomain.CurrentDomain.BaseDirectory;

    public static string StorageRootPath { get; } = Path.Combine(BasePath, "DataStorage");
    public static string DataPath { get; } = Path.Combine(BasePath, "Data");
    public static string ArchivePath { get; } = Path.Combine(DataPath, ArchiveFileName);
    public static string HashPath { get; } = Path.Combine(DataPath, HashFileName);

    public static async Task<bool> FinalizeArchiveAsync(ILogger logger, CancellationToken ct = default)
    {
        Directory.CreateDirectory(DataPath);
        if (!Directory.Exists(StorageRootPath))
        {
            logger.LogWarning("Skip packaging DuckDB archive because source path does not exist. Path: {Path}", StorageRootPath);
            return false;
        }

        DeleteFileIfExists(ArchivePath);
        DeleteFileIfExists(HashPath);

        try
        {
            await CreateArchiveFromDirectoryAsync(StorageRootPath, ArchivePath, ct);
        }
        catch (OperationCanceledException)
        {
            DeleteFileIfExists(ArchivePath);
            DeleteFileIfExists(HashPath);
            throw;
        }

        string hashText = await ComputeSha256Async(ArchivePath, ct);
        await File.WriteAllTextAsync(HashPath, $"{hashText} *{ArchiveFileName}", Encoding.ASCII, ct);
        return true;
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

    private static async Task CreateArchiveFromDirectoryAsync(string sourceDirectory, string destinationPath, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        await Task.Run(() =>
        {
            bool canceled = false;
            SharpSevenZipCompressor compressor = new()
            {
                ArchiveFormat = OutArchiveFormat.SevenZip,
                DirectoryStructure = true,
                PreserveDirectoryRoot = false,
                EventSynchronization = EventSynchronizationStrategy.AlwaysSynchronous
            };
            SharpSevenZipBase.SetLibraryPath(GetSevenZipLibraryPath());
            compressor.FileCompressionStarted += (_, args) =>
            {
                if (!ct.IsCancellationRequested)
                    return;

                args.Cancel = true;
                canceled = true;
            };
            compressor.Compressing += (_, _) =>
            {
                if (ct.IsCancellationRequested)
                    throw new OperationCanceledException(ct);
            };

            compressor.CompressDirectory(sourceDirectory, destinationPath, string.Empty, "*", true);

            if (canceled || ct.IsCancellationRequested)
                throw new OperationCanceledException(ct);
        }, CancellationToken.None);
    }

    private static string GetSevenZipLibraryPath()
    {
        string architectureFolder = Environment.Is64BitProcess ? "x64" : "x86";
        return Path.Combine(BasePath, architectureFolder, "7z.dll");
    }
}

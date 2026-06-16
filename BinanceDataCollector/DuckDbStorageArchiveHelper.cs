using System.Security.Cryptography;
using System.Text;
using SharpSevenZip;

namespace BinanceDataCollector;

internal static class DuckDbStorageArchiveHelper
{
    public const string ArchiveFileName = "BinanceDataCollector.7z";
    private const string HashFileName = ArchiveFileName + ".sha256";
    private const long ArchiveVolumeSizeBytes = 200L * 1024 * 1024 * 1024;
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

        DeleteArchiveFiles();
        DeleteFileIfExists(HashPath);

        try
        {
            await CreateArchiveFromDirectoryAsync(StorageRootPath, ArchivePath, logger, ct);
        }
        catch (OperationCanceledException)
        {
            DeleteArchiveFiles();
            DeleteFileIfExists(HashPath);
            throw;
        }

        string[] archiveFiles = GetArchiveFiles();
        string[] hashLines = await Task.WhenAll(archiveFiles.Select(async path =>
        {
            string hashText = await ComputeSha256Async(path, ct);
            return $"{hashText} *{Path.GetFileName(path)}";
        }));
        await File.WriteAllLinesAsync(HashPath, hashLines, Encoding.ASCII, ct);
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

    private static async Task CreateArchiveFromDirectoryAsync(string sourceDirectory, string destinationPath, ILogger logger, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();

        await Task.Run(() =>
        {
            bool canceled = false;
            byte lastLoggedPercent = 0;
            string? currentFile = null;
            SharpSevenZipCompressor compressor = new()
            {
                ArchiveFormat = OutArchiveFormat.SevenZip,
                CompressionLevel = CompressionLevel.Fast,
                DirectoryStructure = true,
                PreserveDirectoryRoot = false,
                EventSynchronization = EventSynchronizationStrategy.AlwaysSynchronous,
                VolumeSize = ArchiveVolumeSizeBytes
            };
            ConfigureSevenZipLibraryPath();
            compressor.FileCompressionStarted += (_, args) =>
            {
                currentFile = Path.GetRelativePath(sourceDirectory, args.FileName)
                    .Replace(Path.DirectorySeparatorChar, '/');

                if (!ct.IsCancellationRequested)
                    return;

                args.Cancel = true;
                canceled = true;
            };
            compressor.Compressing += (_, args) =>
            {
                if (ct.IsCancellationRequested)
                    throw new OperationCanceledException(ct);

                byte progress = args.PercentDone;
                byte nextThreshold = (byte)(progress / 5 * 5);
                if (nextThreshold <= lastLoggedPercent || nextThreshold == 0)
                    return;

                lastLoggedPercent = nextThreshold;
                logger.LogInformation(
                    "Packaging DuckDB archive progress: {Progress}%. Current file: {CurrentFile}",
                    nextThreshold,
                    currentFile ?? "<unknown>");
            };

            compressor.CompressDirectory(sourceDirectory, destinationPath, string.Empty, "*", true);

            if (lastLoggedPercent < 100)
            {
                logger.LogInformation(
                    "Packaging DuckDB archive progress: 100%. Current file: {CurrentFile}",
                    currentFile ?? "<completed>");
            }

            if (canceled || ct.IsCancellationRequested)
                throw new OperationCanceledException(ct);
        }, CancellationToken.None);
    }

    private static void DeleteArchiveFiles()
    {
        foreach (string path in GetArchiveFiles())
            DeleteFileIfExists(path);
    }

    private static string[] GetArchiveFiles()
    {
        if (!Directory.Exists(DataPath))
            return [];

        return Directory
            .EnumerateFiles(DataPath, ArchiveFileName + "*", SearchOption.TopDirectoryOnly)
            .Where(path => !string.Equals(Path.GetFileName(path), HashFileName, StringComparison.OrdinalIgnoreCase))
            .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private static void ConfigureSevenZipLibraryPath()
    {
        if (!OperatingSystem.IsWindows())
            return;

        string architectureFolder = Environment.Is64BitProcess ? "x64" : "x86";
        SharpSevenZipBase.SetLibraryPath(Path.Combine(BasePath, architectureFolder, "7z.dll"));
    }
}

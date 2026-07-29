using System.Security.Cryptography;
using System.Text;
using SharpSevenZip;

namespace BinanceDataCollector;

internal static class DuckDbStorageArchiveHelper
{
    public const string ArchiveFileName = "BinanceDataCollector.7z";
    private const string HashFileName = ArchiveFileName + ".sha256";
    private const string DoneFileName = "Done";
    private const long ArchiveVolumeSizeBytes = 100L * 1024 * 1024 * 1024;
    private static readonly string BasePath = AppDomain.CurrentDomain.BaseDirectory;

    public static string StorageRootPath { get; } = Path.Combine(BasePath, "DataStorage");
    public static string DataPath { get; } = Path.Combine(BasePath, "Data");

    public static async Task<bool> FinalizeArchiveAsync(ILogger logger, CancellationToken ct = default)
    {
        Directory.CreateDirectory(DataPath);
        if (!Directory.Exists(StorageRootPath))
        {
            logger.LogWarning("Skip packaging DuckDB archive because source path does not exist. Path: {Path}", StorageRootPath);
            return false;
        }

        string? latestCompletedDirectoryPath = GetLatestCompletedArchiveDirectory();
        string finalDirectoryPath = CreateArchiveDirectoryPath(DateTimeOffset.Now);
        string archivePath = Path.Combine(finalDirectoryPath, ArchiveFileName);
        string hashPath = Path.Combine(finalDirectoryPath, HashFileName);
        string donePath = Path.Combine(finalDirectoryPath, DoneFileName);

        CleanupArchiveDirectories(latestCompletedDirectoryPath, finalDirectoryPath);
        Directory.CreateDirectory(finalDirectoryPath);

        try
        {
            await CreateArchiveFromDirectoryAsync(StorageRootPath, archivePath, logger, ct);
        }
        catch (OperationCanceledException)
        {
            DeleteDirectoryIfExists(finalDirectoryPath);
            throw;
        }

        string[] archiveFiles = GetArchiveFiles(finalDirectoryPath);
        string[] hashLines = await Task.WhenAll(archiveFiles.Select(async path =>
        {
            string hashText = await ComputeSha256Async(path, ct);
            return $"{hashText} *{Path.GetFileName(path)}";
        }));
        await File.WriteAllLinesAsync(hashPath, hashLines, Encoding.ASCII, ct);
        await File.WriteAllBytesAsync(donePath, [], ct);

        CleanupArchiveDirectories(finalDirectoryPath, null);
        return true;
    }

    private static void DeleteFileIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }

    private static void DeleteDirectoryIfExists(string path)
    {
        if (Directory.Exists(path))
            Directory.Delete(path, true);
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
                CompressionLevel = CompressionLevel.Low,
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

    private static string CreateArchiveDirectoryPath(DateTimeOffset timestamp)
    {
        string directoryName = timestamp.ToString("yyyyMMdd-HHmm");
        string directoryPath = Path.Combine(DataPath, directoryName);
        if (!Directory.Exists(directoryPath))
            return directoryPath;

        for (int suffix = 1; ; suffix++)
        {
            string candidatePath = Path.Combine(DataPath, $"{directoryName}-{suffix:D2}");
            if (Directory.Exists(candidatePath))
                continue;

            return candidatePath;
        }
    }

    private static string[] GetArchiveFiles(string directoryPath)
    {
        if (!Directory.Exists(directoryPath))
            return [];

        return Directory
            .EnumerateFiles(directoryPath, ArchiveFileName + "*", SearchOption.TopDirectoryOnly)
            .Where(path => !string.Equals(Path.GetFileName(path), HashFileName, StringComparison.OrdinalIgnoreCase))
            .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private static string? GetLatestCompletedArchiveDirectory()
    {
        return Directory
            .EnumerateDirectories(DataPath, "*", SearchOption.TopDirectoryOnly)
            .Where(IsCompletedArchiveDirectory)
            .OrderByDescending(path => Path.GetFileName(path), StringComparer.OrdinalIgnoreCase)
            .FirstOrDefault();
    }

    private static bool IsCompletedArchiveDirectory(string directoryPath)
        => File.Exists(Path.Combine(directoryPath, HashFileName))
            && File.Exists(Path.Combine(directoryPath, DoneFileName));

    private static void CleanupArchiveDirectories(string? completedDirectoryToKeep, string? inProgressDirectoryToKeep)
    {
        foreach (string directoryPath in Directory.EnumerateDirectories(DataPath, "*", SearchOption.TopDirectoryOnly))
        {
            string directoryName = Path.GetFileName(directoryPath);
            if (!string.IsNullOrEmpty(completedDirectoryToKeep) &&
                string.Equals(directoryPath, completedDirectoryToKeep, StringComparison.OrdinalIgnoreCase))
                continue;

            if (!string.IsNullOrEmpty(inProgressDirectoryToKeep) &&
                string.Equals(directoryPath, inProgressDirectoryToKeep, StringComparison.OrdinalIgnoreCase))
                continue;

            Directory.Delete(directoryPath, true);
        }
    }

    private static void ConfigureSevenZipLibraryPath()
    {
        if (!OperatingSystem.IsWindows())
            return;

        string architectureFolder = Environment.Is64BitProcess ? "x64" : "x86";
        SharpSevenZipBase.SetLibraryPath(Path.Combine(BasePath, architectureFolder, "7z.dll"));
    }
}

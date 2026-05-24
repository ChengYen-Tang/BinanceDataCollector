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

        if (Directory.Exists(TmpPath) && !Directory.EnumerateFileSystemEntries(TmpPath).Any())
            Directory.Delete(TmpPath, true);
    }

    public static async Task FinalizeArchiveAsync(CancellationToken ct = default)
    {
        Directory.CreateDirectory(DataPath);
        Directory.CreateDirectory(TmpPath);

        string archiveStagingPath = Path.Combine(TmpPath, ArchiveFileName + ".tmp");
        string hashStagingPath = Path.Combine(TmpPath, HashFileName + ".tmp");
        string archiveBackupPath = Path.Combine(TmpPath, ArchiveFileName + ".bak");
        string hashBackupPath = Path.Combine(TmpPath, HashFileName + ".bak");

        try
        {
            DeleteFileIfExists(archiveStagingPath);
            DeleteFileIfExists(hashStagingPath);
            DeleteFileIfExists(archiveBackupPath);
            DeleteFileIfExists(hashBackupPath);

            ZipFile.CreateFromDirectory(WorkRootPath, archiveStagingPath, CompressionLevel.Optimal, includeBaseDirectory: false);

            string hashText = await ComputeSha256Async(archiveStagingPath, ct);
            await File.WriteAllTextAsync(hashStagingPath, $"{hashText} *{ArchiveFileName}", Encoding.ASCII, ct);

            PublishArtifacts(archiveStagingPath, hashStagingPath, archiveBackupPath, hashBackupPath);
        }
        finally
        {
            DeleteFileIfExists(archiveStagingPath);
            DeleteFileIfExists(hashStagingPath);
            DeleteFileIfExists(archiveBackupPath);
            DeleteFileIfExists(hashBackupPath);
        }
    }

    private static void PublishArtifacts(string archiveStagingPath, string hashStagingPath, string archiveBackupPath, string hashBackupPath)
    {
        bool archivePublished = false;
        bool hashPublished = false;
        try
        {
            ReplaceFile(archiveStagingPath, ArchivePath, archiveBackupPath);
            archivePublished = true;
            ReplaceFile(hashStagingPath, HashPath, hashBackupPath);
            hashPublished = true;
        }
        catch
        {
            if (hashPublished)
                RollbackFile(HashPath, hashBackupPath);
            else
                RollbackCreatedFile(HashPath, hashBackupPath);

            if (archivePublished)
                RollbackFile(ArchivePath, archiveBackupPath);
            else
                RollbackCreatedFile(ArchivePath, archiveBackupPath);

            throw;
        }

        DeleteFileIfExists(archiveBackupPath);
        DeleteFileIfExists(hashBackupPath);
    }

    private static void ReplaceFile(string sourcePath, string destinationPath, string backupPath)
    {
        if (File.Exists(destinationPath))
        {
            File.Replace(sourcePath, destinationPath, backupPath, ignoreMetadataErrors: true);
            return;
        }

        File.Move(sourcePath, destinationPath);
    }

    private static void RollbackFile(string destinationPath, string backupPath)
    {
        if (!File.Exists(backupPath))
            return;

        if (File.Exists(destinationPath))
            File.Replace(backupPath, destinationPath, null, ignoreMetadataErrors: true);
        else
            File.Move(backupPath, destinationPath);
    }

    private static void RollbackCreatedFile(string destinationPath, string backupPath)
    {
        DeleteFileIfExists(destinationPath);
        DeleteFileIfExists(backupPath);
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

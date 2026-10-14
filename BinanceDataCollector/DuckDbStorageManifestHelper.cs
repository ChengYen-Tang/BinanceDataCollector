using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace BinanceDataCollector;

internal static class DuckDbStorageManifestHelper
{
    private const int ManifestVersion = 1;
    private const string ManifestFileName = "DataStorageManifest.json";
    private static readonly StringComparer PathComparer = OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.General)
    {
        WriteIndented = true
    };

    private static string StorageRootPath => DuckDbStorageHelper.StorageRootPath;
    private static string ManifestPath => Path.Combine(StorageRootPath, ManifestFileName);

    public static async Task WriteAsync(ILogger logger, CancellationToken ct = default)
    {
        Directory.CreateDirectory(StorageRootPath);

        List<DuckDbStorageManifestFile> files = [];
        foreach (string databasePath in EnumerateDatabasePaths())
        {
            ct.ThrowIfCancellationRequested();
            files.Add(new(GetRelativePath(databasePath), await ComputeSha256Async(databasePath, ct)));
        }

        DuckDbStorageManifest manifest = new()
        {
            Version = ManifestVersion,
            GeneratedAtUtc = DateTimeOffset.UtcNow,
            Files = files
        };

        string temporaryPath = ManifestPath + ".tmp";
        try
        {
            string json = JsonSerializer.Serialize(manifest, JsonOptions);
            await File.WriteAllTextAsync(temporaryPath, json, Encoding.UTF8, ct);
            File.Move(temporaryPath, ManifestPath, true);
        }
        finally
        {
            DeleteFileIfExists(temporaryPath);
        }

        logger.LogInformation(
            "DuckDB storage manifest written. FileCount: {FileCount}, Path: {Path}",
            files.Count,
            ManifestPath);
    }

    public static async Task ValidateAsync(ILogger logger, CancellationToken ct = default)
    {
        if (!File.Exists(ManifestPath))
            throw new FileNotFoundException("DuckDB storage manifest was not found.", ManifestPath);

        DuckDbStorageManifest? manifest;
        try
        {
            string json = await File.ReadAllTextAsync(ManifestPath, ct);
            manifest = JsonSerializer.Deserialize<DuckDbStorageManifest>(json, JsonOptions);
        }
        catch (JsonException ex)
        {
            throw new InvalidDataException($"DuckDB storage manifest is invalid: {ManifestPath}", ex);
        }

        if (manifest is null || manifest.Files is null)
            throw new InvalidDataException($"DuckDB storage manifest is invalid: {ManifestPath}");

        if (manifest.Version != ManifestVersion)
            throw new InvalidDataException($"Unsupported DuckDB storage manifest version: {manifest.Version}");

        HashSet<string> validatedPaths = new(PathComparer);
        foreach (DuckDbStorageManifestFile? file in manifest.Files)
        {
            ct.ThrowIfCancellationRequested();
            if (file is null)
                throw new InvalidDataException($"DuckDB storage manifest contains a null file entry: {ManifestPath}");

            string databasePath = ResolveDatabasePath(file.Path);
            if (!validatedPaths.Add(databasePath))
                throw new InvalidDataException($"DuckDB storage manifest contains a duplicate file entry: {file.Path}");

            if (!File.Exists(databasePath))
                throw new InvalidDataException($"DuckDB file recorded in the manifest is missing: {file.Path}");

            string actualHash = await ComputeSha256Async(databasePath, ct);
            if (!string.Equals(actualHash, file.Sha256, StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidDataException(
                    $"DuckDB file hash mismatch. File: {file.Path}, Expected: {file.Sha256}, Actual: {actualHash}");
            }
        }

        logger.LogInformation(
            "DuckDB storage manifest validated. FileCount: {FileCount}, Path: {Path}",
            manifest.Files.Count,
            ManifestPath);
    }

    private static IEnumerable<string> EnumerateDatabasePaths()
        => !Directory.Exists(StorageRootPath)
            ? []
            : Directory
                .EnumerateFiles(StorageRootPath, "*.duckdb", SearchOption.AllDirectories)
                .OrderBy(path => path, PathComparer);

    private static string GetRelativePath(string databasePath)
        => Path.GetRelativePath(StorageRootPath, databasePath).Replace('\\', '/');

    private static string ResolveDatabasePath(string? relativePath)
    {
        if (string.IsNullOrWhiteSpace(relativePath) || Path.IsPathRooted(relativePath))
            throw new InvalidDataException($"DuckDB storage manifest contains an invalid path: {relativePath}");

        string normalizedRelativePath = relativePath
            .Replace('/', Path.DirectorySeparatorChar)
            .Replace('\\', Path.DirectorySeparatorChar);
        string databasePath = Path.GetFullPath(Path.Combine(StorageRootPath, normalizedRelativePath));
        string storageRootPath = Path.TrimEndingDirectorySeparator(Path.GetFullPath(StorageRootPath)) + Path.DirectorySeparatorChar;

        if (!databasePath.StartsWith(storageRootPath, GetPathComparison()) ||
            !databasePath.EndsWith(".duckdb", StringComparison.OrdinalIgnoreCase))
            throw new InvalidDataException($"DuckDB storage manifest contains an invalid path: {relativePath}");

        return databasePath;
    }

    private static StringComparison GetPathComparison()
        => OperatingSystem.IsWindows() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;

    private static async Task<string> ComputeSha256Async(string path, CancellationToken ct)
    {
        await using FileStream stream = File.OpenRead(path);
        byte[] hash = await SHA256.HashDataAsync(stream, ct);
        return Convert.ToHexString(hash);
    }

    private static void DeleteFileIfExists(string path)
    {
        if (File.Exists(path))
            File.Delete(path);
    }
}

internal sealed class DuckDbStorageManifest
{
    public int Version { get; set; }
    public DateTimeOffset GeneratedAtUtc { get; set; }
    public List<DuckDbStorageManifestFile>? Files { get; set; }
}

internal sealed class DuckDbStorageManifestFile
{
    public DuckDbStorageManifestFile()
    {
    }

    public DuckDbStorageManifestFile(string path, string sha256) => (Path, Sha256) = (path, sha256);

    public string? Path { get; set; }
    public string? Sha256 { get; set; }
}

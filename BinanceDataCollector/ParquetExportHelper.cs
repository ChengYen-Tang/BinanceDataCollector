using Parquet;
using Parquet.Serialization;

namespace BinanceDataCollector;

internal sealed class ParquetExportOptions
{
    public required CompressionMethod CompressionMethod { get; init; }
    public int? RowGroupSize { get; init; }
}

internal static class ParquetExportHelper
{
    public static async Task ExportAsync<T>(string path, IEnumerable<T> records, ParquetExportOptions options, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);

        await using FileStream stream = File.Create(path);
        ParquetOptions parquetOptions = new()
        {
            CompressionMethod = options.CompressionMethod,
        };
        if (options.RowGroupSize.HasValue)
            parquetOptions.RowGroupSize = options.RowGroupSize.Value;

        await ParquetSerializer.SerializeAsync(records.ToArray(), stream, parquetOptions, cancellationToken: ct);
    }
}

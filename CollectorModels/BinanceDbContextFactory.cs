using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

namespace CollectorModels;

public class BinanceDbContextFactory : IDesignTimeDbContextFactory<BinanceDbContext>
{
    public BinanceDbContext CreateDbContext(string[] args)
    {
        DbContextOptionsBuilder<BinanceDbContext> builder = new();
        builder.UseSqlServer("Server=(localdb)\\mssqllocaldb;Database=BinanceDataCollectorDesignTime;Trusted_Connection=True;TrustServerCertificate=True");
        return new BinanceDbContext(builder.Options);
    }
}

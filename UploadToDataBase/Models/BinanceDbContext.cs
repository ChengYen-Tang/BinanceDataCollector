using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations;

namespace UploadToDataBase.Models
{
    internal class BinanceDbContext : DbContext
    {
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlServer("Data Source=172.30.1.253,1433;Initial Catalog=CryptocurrencyDataset;User ID=sa;Password=P@ssw0rd;", opts => opts.CommandTimeout((int)TimeSpan.FromMinutes(30).TotalSeconds));
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<CoinDataModel>()
                .HasOne(coinData => coinData.Coin)
                .WithMany(coin => coin.CoinData)
                .HasForeignKey(coinData => coinData.CoinId);
        }

        public virtual DbSet<CoinModel> Coins { get; set; }
        public virtual DbSet<CoinDataModel> CoinData { get; set; }
    }

    public class CoinModel
    {
        public CoinModel()
            => (Id, CoinData) = (Guid.NewGuid(), new List<CoinDataModel>());

        [Key]
        [Required]
        public Guid Id {  get; set; }
        [Required]
        public string Name {  get; set; }

        public ICollection<CoinDataModel> CoinData {  get; set; }
    }

    public class CoinDataModel
    {
        [Key]
        [Required]
        public string Key {  get; set; }
        [Required]
        public DateTime Date { get; set; }
        [Required]
        public decimal Open { get; set; }
        [Required]
        public decimal High { get; set; }
        [Required]
        public decimal Low { get; set; }
        [Required]
        public decimal Close { get; set; }
        [Required]
        public decimal Volume { get; set; }
        [Required]
        public decimal Money { get; set; }
        [Required]
        public int TradeCount { get; set; }
        [Required]
        public decimal TakerBuyBaseVolume { get; set; }
        [Required]
        public decimal TakerBuyQuoteVolume { get; set; }

        [Required]
        public Guid CoinId {  get; set; }
        [Required]
        public virtual CoinModel Coin {  get; set; }
    }
}

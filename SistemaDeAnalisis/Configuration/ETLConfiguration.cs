namespace SistemaDeAnalisis.Configuration
{
    public class ETLConfiguration
    {
        public string DataDirectory { get; set; } = "Data";
        public string ConnectionString { get; set; } = string.Empty;
        public string ApiUrl { get; set; } = string.Empty;
        public int BatchSize { get; set; } = 1000;
    }
}
using SistemaDeAnalisis.Interfaces;
using SistemaDeAnalisis.Models;
using SistemaDeAnalisis.Configuration; 
using Microsoft.Extensions.Options;

namespace SistemaDeAnalisis.Extractors
{
    public class ApiExtractor : IExtractor
    {
        private readonly ILogger<ApiExtractor> _logger;
        private readonly HttpClient _httpClient;
        private readonly ETLConfiguration _config;

        public string SourceType => "API";

        public ApiExtractor(ILogger<ApiExtractor> logger, HttpClient httpClient, IOptions<ETLConfiguration> config)
        {
            _logger = logger;
            _httpClient = httpClient;
            _config = config.Value;
        }

        public async Task<IEnumerable<SalesData>> ExtractAsync()
        {
            try
            {
               
                var simulatedData = new List<SalesData>
                {
                    new() { Id = 1001, CustomerID = 1, ProductID = 1, Quantity = 2, TotalPrice = 1750.78m, Source = "API", CreatedDate = DateTime.UtcNow },
                    new() { Id = 1002, CustomerID = 2, ProductID = 3, Quantity = 1, TotalPrice = 90.81m, Source = "API", CreatedDate = DateTime.UtcNow },
                    new() { Id = 1003, CustomerID = 3, ProductID = 5, Quantity = 3, TotalPrice = 2120.34m, Source = "API", CreatedDate = DateTime.UtcNow }
                };

                await Task.Delay(100); 

                _logger.LogInformation("Se extrajeron {Count} registros de ApiExtractor", simulatedData.Count);
                return simulatedData;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al extraer datos de la API");
                return new List<SalesData>();
            }
        }
    }
}
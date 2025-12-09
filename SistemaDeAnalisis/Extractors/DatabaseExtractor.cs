using SistemaDeAnalisis.Interfaces;
using SistemaDeAnalisis.Models;
using SistemaDeAnalisis.Configuration; 
using Microsoft.Extensions.Options;
using Npgsql;
using Dapper;

namespace SistemaDeAnalisis.Extractors
{
    public class DatabaseExtractor : IExtractor
    {
        private readonly ILogger<DatabaseExtractor> _logger;
        private readonly ETLConfiguration _config;

        public string SourceType => "Database";

        public DatabaseExtractor(ILogger<DatabaseExtractor> logger, IOptions<ETLConfiguration> config)
        {
            _logger = logger;
            _config = config.Value;
        }

        public async Task<IEnumerable<SalesData>> ExtractAsync()
        {
            try
            {
                using var connection = new NpgsqlConnection(_config.ConnectionString);

                const string query = @"
                    SELECT 
                        id as Id,
                        customerid as CustomerID,
                        firstname as FirstName,
                        lastname as LastName,
                        email as Email,
                        productid as ProductID,
                        productname as ProductName,
                        category as Category,
                        price as Price,
                        quantity as Quantity,
                        totalprice as TotalPrice,
                        orderdate as OrderDate,
                        source as Source,
                        createddate as CreatedDate
                    FROM staging_salesdata 
                    WHERE orderdate >= CURRENT_DATE - INTERVAL '30 days'";

                var salesData = await connection.QueryAsync<SalesData>(query);
                var result = salesData.ToList();

                _logger.LogInformation("Se extrajeron {Count} registros de DatabaseExtractor", result.Count);
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al extraer datos de la base de datos");
                return new List<SalesData>();
            }
        }
    }
}
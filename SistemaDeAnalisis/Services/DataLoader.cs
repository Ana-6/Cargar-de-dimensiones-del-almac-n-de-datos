using SistemaDeAnalisis.Models;
using SistemaDeAnalisis.Configuration;
using Microsoft.Extensions.Options;
using Npgsql;
using Dapper;

namespace SistemaDeAnalisis.Services
{
    public class DataLoader
    {
        private readonly ILogger<DataLoader> _logger;
        private readonly ETLConfiguration _config;

        public DataLoader(ILogger<DataLoader> logger, IOptions<ETLConfiguration> config)
        {
            _logger = logger;
            _config = config.Value;
        }

        public async Task<bool> LoadDataAsync(IEnumerable<SalesData> data)
        {
            try
            {
                if (!data.Any())
                {
                    _logger.LogWarning("No hay datos para cargar");
                    return true;
                }

                using var connection = new NpgsqlConnection(_config.ConnectionString);
                await connection.OpenAsync();

                // Estrategia: UPSERT (insertar o actualizar si existe)
                const string upsertSql = @"
                    INSERT INTO staging_salesdata (
                        id, customerid, firstname, lastname, email, 
                        productid, productname, category, price, quantity, 
                        totalprice, orderdate, source, createddate
                    ) VALUES (
                        @Id, @CustomerID, @FirstName, @LastName, @Email,
                        @ProductID, @ProductName, @Category, @Price, @Quantity,
                        @TotalPrice, @OrderDate, @Source, @CreatedDate
                    )
                    ON CONFLICT (id) 
                    DO UPDATE SET
                        customerid = EXCLUDED.customerid,
                        firstname = EXCLUDED.firstname,
                        lastname = EXCLUDED.lastname,
                        email = EXCLUDED.email,
                        productid = EXCLUDED.productid,
                        productname = EXCLUDED.productname,
                        category = EXCLUDED.category,
                        price = EXCLUDED.price,
                        quantity = EXCLUDED.quantity,
                        totalprice = EXCLUDED.totalprice,
                        orderdate = EXCLUDED.orderdate,
                        source = EXCLUDED.source,
                        createddate = EXCLUDED.createddate";

                var affectedRows = await connection.ExecuteAsync(upsertSql, data);

                _logger.LogInformation("Cargados/Actualizados {Count} registros en la base de datos", affectedRows);

                await LoadToAnalyticalTable(connection);

                return true;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al cargar datos en la base de datos");
                return false;
            }
        }

        private async Task LoadToAnalyticalTable(NpgsqlConnection connection)
        {
            await connection.ExecuteAsync("DELETE FROM analytical_sales WHERE source != 'API'");

            const string transformSql = @"
                INSERT INTO analytical_sales (
                    customerid, customername, productid, productname, 
                    category, quantity, unitprice, totalprice, saledate, source
                )
                SELECT 
                    customerid,
                    CONCAT(firstname, ' ', lastname) as customername,
                    productid,
                    productname,
                    category,
                    quantity,
                    price as unitprice,
                    totalprice,
                    COALESCE(orderdate, createddate) as saledate,
                    source
                FROM staging_salesdata
                WHERE customerid IS NOT NULL AND productid IS NOT NULL";

            var affectedRows = await connection.ExecuteAsync(transformSql);
            _logger.LogInformation("Transformados {Count} registros a tabla analítica", affectedRows);
        }
    }
}
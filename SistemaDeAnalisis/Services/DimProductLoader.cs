using Dapper;
using Npgsql;
using SistemaDeAnalisis.Models;

namespace SistemaDeAnalisis.Services
{
    public class DimProductLoader
    {
        private readonly string _conn;
        public DimProductLoader(string conn) => _conn = conn;

        public async Task LoadAsync(IEnumerable<DimProduct> products)
        {
            using var connection = new NpgsqlConnection(_conn);
            await connection.OpenAsync();

            var sql = @"
                INSERT INTO dim_product (productid, productname, category, price, stock)
                VALUES (@ProductID, @ProductName, @Category, @Price, @Stock)
                ON CONFLICT (productid) DO UPDATE SET
                    productname = EXCLUDED.productname,
                    category = EXCLUDED.category,
                    price = EXCLUDED.price,
                    stock = EXCLUDED.stock;
            ";

            await connection.ExecuteAsync(sql, products);
        }
    }
}

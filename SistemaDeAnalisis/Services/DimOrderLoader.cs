using Dapper;
using Npgsql;
using SistemaDeAnalisis.Models;

namespace SistemaDeAnalisis.Services
{
    public class DimOrderLoader
    {
        private readonly string _conn;
        public DimOrderLoader(string conn) => _conn = conn;

        public async Task LoadAsync(IEnumerable<DimOrder> orders)
        {
            using var connection = new NpgsqlConnection(_conn);
            await connection.OpenAsync();

            var sql = @"
                INSERT INTO dim_order (orderid, customerid, orderdate, status)
                VALUES (@OrderID, @CustomerID, @OrderDate, @Status)
                ON CONFLICT (orderid) DO UPDATE SET
                    customerid = EXCLUDED.customerid,
                    orderdate = EXCLUDED.orderdate,
                    status = EXCLUDED.status;
            ";

            await connection.ExecuteAsync(sql, orders);
        }
    }
}

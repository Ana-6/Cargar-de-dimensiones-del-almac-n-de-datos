using Dapper;
using Npgsql;
using SistemaDeAnalisis.Models;

namespace SistemaDeAnalisis.Services
{
    public class DimCustomerLoader
    {
        private readonly string _conn;
        public DimCustomerLoader(string conn) => _conn = conn;

        public async Task LoadAsync(IEnumerable<DimCustomer> customers)
        {
            using var connection = new NpgsqlConnection(_conn);
            await connection.OpenAsync();

            var sql = @"
                INSERT INTO dim_customer (customerid, firstname, lastname, email, phone, city, country)
                VALUES (@CustomerID, @FirstName, @LastName, @Email, @Phone, @City, @Country)
                ON CONFLICT (customerid) DO UPDATE SET
                    firstname = EXCLUDED.firstname,
                    lastname = EXCLUDED.lastname,
                    email = EXCLUDED.email,
                    phone = EXCLUDED.phone,
                    city = EXCLUDED.city,
                    country = EXCLUDED.country;
            ";

            await connection.ExecuteAsync(sql, customers);
        }
    }
}

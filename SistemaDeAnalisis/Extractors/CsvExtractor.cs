using CsvHelper;
using CsvHelper.Configuration;
using SistemaDeAnalisis.Interfaces;
using SistemaDeAnalisis.Models;
using SistemaDeAnalisis.Configuration;
using Microsoft.Extensions.Options;
using System.Globalization;

namespace SistemaDeAnalisis.Extractors
{
    public class CsvExtractor : IExtractor
    {
        private readonly ILogger<CsvExtractor> _logger;
        private readonly ETLConfiguration _config;

        public string SourceType => "CSV";

        public CsvExtractor(ILogger<CsvExtractor> logger, IOptions<ETLConfiguration> config)
        {
            _logger = logger;
            _config = config.Value;
        }

        public async Task<IEnumerable<SalesData>> ExtractAsync()
        {
            var allSalesData = new List<SalesData>();

            try
            {
                if (!Directory.Exists(_config.DataDirectory))
                {
                    _logger.LogWarning("Directorio Data no encontrado: {Directory}", _config.DataDirectory);
                    return allSalesData;
                }

                _logger.LogInformation("Buscando archivos CSV en directorio: {Directory}", _config.DataDirectory);

                var customers = await ProcessCsvFile<CustomerData>("customers.csv");
                var products = await ProcessCsvFile<ProductData>("products.csv");
                var orders = await ProcessCsvFile<OrderData>("orders.csv");
                var orderDetails = await ProcessCsvFile<OrderDetailData>("order_details.csv");

                allSalesData = EnrichSalesData(customers, products, orders, orderDetails);

                _logger.LogInformation("Procesados: {Customers} clientes, {Products} productos, {Orders} órdenes, {Details} detalles",
                    customers.Count, products.Count, orders.Count, orderDetails.Count);

                return allSalesData;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error al extraer datos CSV");
                return new List<SalesData>();
            }
        }
        private async Task<List<T>> ProcessCsvFile<T>(string fileName)
        {
            var filePath = Path.Combine(_config.DataDirectory, fileName);

            if (!File.Exists(filePath))
            {
                _logger.LogWarning("Archivo no encontrado: {File}", filePath);
                return new List<T>();
            }

            try
            {
                var config = new CsvConfiguration(CultureInfo.InvariantCulture)
                {
                    HasHeaderRecord = true,
                    Delimiter = ",",
                    MissingFieldFound = null,
                    HeaderValidated = null
                };

                using var reader = new StreamReader(filePath);
                using var csv = new CsvReader(reader, config);

                var records = csv.GetRecords<T>().ToList();

                _logger.LogInformation("Se extrajeron {Count} registros de {File}", records.Count, fileName);

                return records;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error procesando archivo {File}", fileName);
                return new List<T>();
            }
        }

        private List<SalesData> EnrichSalesData(
            List<CustomerData> customers,
            List<ProductData> products,
            List<OrderData> orders,
            List<OrderDetailData> orderDetails)
        {
            var salesData = new List<SalesData>();

            var productDict = products.ToDictionary(p => p.ProductID);
            var customerDict = customers.ToDictionary(c => c.CustomerID);

            var usedIds = new HashSet<int>();
            var random = new Random();

            foreach (var detail in orderDetails)
            {
                int uniqueId;
                do
                {
                    uniqueId = detail.OrderID * 1000 + detail.ProductID + random.Next(1, 1000);
                } while (usedIds.Contains(uniqueId));
                usedIds.Add(uniqueId);

                var sale = new SalesData
                {
                    Id = uniqueId,
                    CustomerID = GetCustomerIdFromOrder(detail.OrderID, customerDict),
                    OrderID = detail.OrderID,
                    ProductID = detail.ProductID,
                    Quantity = detail.Quantity,
                    TotalPrice = detail.TotalPrice,
                    Source = "order_details.csv",
                    CreatedDate = DateTime.UtcNow,
                    OrderDate = DateTime.UtcNow.AddDays(-random.Next(1, 365))
                };

                if (productDict.TryGetValue(detail.ProductID, out var product))
                {
                    sale.ProductName = product.ProductName;
                    sale.Category = product.Category;
                    sale.Price = product.Price;
                }

                if (customerDict.TryGetValue(sale.CustomerID, out var customer))
                {
                    sale.FirstName = customer.FirstName;
                    sale.LastName = customer.LastName;
                    sale.Email = customer.Email;
                }

                salesData.Add(sale);
            }

            return salesData;
        }

        private int GetCustomerIdFromOrder(int orderId, Dictionary<int, CustomerData> customers)
        {
            if (customers.Count == 0)
                return 1;

            var customerIds = customers.Keys.ToList();
            return customerIds[orderId % customerIds.Count];
        }

        // ---------------------------------------------------------------
        //  ***** MÉTODO NUEVO: EXTRACCIÓN DE DIMENSIONES *****
        // ---------------------------------------------------------------
        public async Task<CsvExtractionResult> ExtractWithDimensionsAsync()
        {
            var result = new CsvExtractionResult();

            try
            {
                if (!Directory.Exists(_config.DataDirectory))
                {
                    _logger.LogWarning("Directorio Data no encontrado: {Directory}", _config.DataDirectory);
                    return result;
                }

                var customers = await ProcessCsvFile<CustomerData>("customers.csv");
                result.Customers = customers.Select(c => new DimCustomer
                {
                    CustomerID = c.CustomerID,
                    FirstName = c.FirstName,
                    LastName = c.LastName,
                    Email = c.Email,
                    Phone = c.Phone,
                    City = c.City,
                    Country = c.Country
                }).ToList();

                var products = await ProcessCsvFile<ProductData>("products.csv");
                result.Products = products.Select(p => new DimProduct
                {
                    ProductID = p.ProductID,
                    ProductName = p.ProductName,
                    Category = p.Category,
                    Price = p.Price,
                    Stock = p.Stock
                }).ToList();

                var ordersPath = Path.Combine(_config.DataDirectory, "orders.csv");
                if (File.Exists(ordersPath))
                {
                    var cfg = new CsvConfiguration(CultureInfo.InvariantCulture)
                    {
                        HasHeaderRecord = true,
                        MissingFieldFound = null,
                        HeaderValidated = null
                    };

                    using var reader = new StreamReader(ordersPath);
                    using var csv = new CsvReader(reader, cfg);

                    result.Orders = csv.GetRecords<DimOrder>().ToList();
                }

                var orderDetails = await ProcessCsvFile<OrderDetailData>("order_details.csv");

                var productsDict = products.ToDictionary(p => p.ProductID);
                var customersDict = customers.ToDictionary(c => c.CustomerID);

                var sales = new List<SalesData>();
                var random = new Random();
                var usedIds = new HashSet<int>();

                foreach (var od in orderDetails)
                {
                    int uniqueId;
                    do
                    {
                        uniqueId = od.OrderID * 1000 + od.ProductID + random.Next(1, 1000);
                    } while (usedIds.Contains(uniqueId));
                    usedIds.Add(uniqueId);

                    var sale = new SalesData
                    {
                        Id = uniqueId,
                        OrderID = od.OrderID,
                        ProductID = od.ProductID,
                        Quantity = od.Quantity,
                        TotalPrice = od.TotalPrice,
                        Source = "order_details.csv",
                        CreatedDate = DateTime.UtcNow,
                        OrderDate = DateTime.UtcNow.AddDays(-random.Next(1, 365))
                    };

                    if (productsDict.TryGetValue(od.ProductID, out var prod))
                    {
                        sale.ProductName = prod.ProductName;
                        sale.Category = prod.Category;
                        sale.Price = prod.Price;
                    }

                    sale.CustomerID = result.Orders
                        .FirstOrDefault(o => o.OrderID == od.OrderID)?.CustomerID
                        ?? customersDict.Values.First().CustomerID;

                    sales.Add(sale);
                }

                result.Sales = sales;

                _logger.LogInformation("CsvExtractor: Dims extraídas correctamente.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error en ExtractWithDimensionsAsync");
            }

            return result;
        }
    }

    // ------------------------------
    // DTO DEL RESULTADO FINAL
    // ------------------------------
    public class CsvExtractionResult
    {
        public List<DimCustomer> Customers { get; set; } = new();
        public List<DimProduct> Products { get; set; } = new();
        public List<DimOrder> Orders { get; set; } = new();
        public List<SalesData> Sales { get; set; } = new();
    }
}

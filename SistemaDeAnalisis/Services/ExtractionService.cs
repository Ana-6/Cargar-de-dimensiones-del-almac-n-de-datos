using SistemaDeAnalisis.Extractors;
using SistemaDeAnalisis.Interfaces;
using SistemaDeAnalisis.Models;
using SistemaDeAnalisis.Services;

namespace SistemaDeAnalisis.Services
{
    public class ExtractionService
    {
        private readonly ILogger<ExtractionService> _logger;
        private readonly IEnumerable<IExtractor> _extractors;

        // NUEVOS LOADER PARA DIMENSIONES
        private readonly DimCustomerLoader _customerLoader;
        private readonly DimProductLoader _productLoader;
        private readonly DimOrderLoader _orderLoader;

        private readonly DataLoader _dataLoader;

        public ExtractionService(
            ILogger<ExtractionService> logger,
            IEnumerable<IExtractor> extractors,
            DataLoader dataLoader,
            DimCustomerLoader customerLoader,
            DimProductLoader productLoader,
            DimOrderLoader orderLoader)
        {
            _logger = logger;
            _extractors = extractors;
            _dataLoader = dataLoader;

            _customerLoader = customerLoader;
            _productLoader = productLoader;
            _orderLoader = orderLoader;

            _logger.LogInformation("ExtractionService inicializado con {Count} extractors", _extractors?.Count() ?? 0);
        }

        public async Task ExecuteExtractionAsync()
        {
            _logger.LogInformation("=== INICIO DEL PROCESO ETL ===");

            var allData = new List<SalesData>();
            CsvExtractionResult? csvDims = null;

            try
            {
                if (_extractors == null || !_extractors.Any())
                {
                    _logger.LogError("NO SE ENCONTRARON EXTRACTORS REGISTRADOS");
                    return;
                }

                _logger.LogInformation("Extractors encontrados: {Count}", _extractors.Count());

                foreach (var extractor in _extractors)
                {
                    _logger.LogInformation("Procesando extractor: {Extractor}", extractor.GetType().Name);

                    if (extractor.GetType().Name == "CsvExtractor")
                    {
                        _logger.LogInformation("CsvExtractor detectado. Extrayendo DIMENSIONES…");

                        var dynamicExtractor = extractor as dynamic;

                        csvDims = await dynamicExtractor.ExtractWithDimensionsAsync();

                        allData.AddRange(csvDims.Sales);

                        _logger.LogInformation(
                            "CsvExtractor produjo {Cust} clientes, {Prod} productos, {Ord} órdenes y {Sales} ventas",
                            csvDims.Customers.Count,
                            csvDims.Products.Count,
                            csvDims.Orders.Count,
                            csvDims.Sales.Count
                        );

                        continue;
                    }

                    var data = await ExtractFromSourceAsync(extractor);
                    allData.AddRange(data);
                }

                // === CARGA DE DIMENSIONES ===
                if (csvDims != null)
                {
                    _logger.LogInformation("=== CARGANDO DIMENSIONES EN EL DATA WAREHOUSE ===");

                    await _customerLoader.LoadAsync(csvDims.Customers);
                    await _productLoader.LoadAsync(csvDims.Products);
                    await _orderLoader.LoadAsync(csvDims.Orders);

                    _logger.LogInformation("Dimensiones cargadas correctamente.");
                }
                else
                {
                    _logger.LogWarning("No se encontraron dimensiones CSV para cargar.");
                }

                _logger.LogInformation("ETL FINALIZADO (DIMENSIONES LISTAS).");
                _logger.LogInformation("Los FACTS NO se cargaron.");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ERROR DURANTE EL PROCESO ETL");
            }
        }

        private async Task<IEnumerable<SalesData>> ExtractFromSourceAsync(IExtractor extractor)
        {
            try
            {
                _logger.LogInformation("Ejecutando extractor: {ExtractorName}", extractor.GetType().Name);
                var data = await extractor.ExtractAsync();
                _logger.LogInformation("{ExtractorName} extrajo {Count} registros",
                    extractor.GetType().Name, data.Count());
                return data;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ERROR en {ExtractorName}", extractor.GetType().Name);
                return new List<SalesData>();
            }
        }
    }
}

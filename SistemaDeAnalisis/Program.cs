using SistemaDeAnalisis;
using SistemaDeAnalisis.Configuration;
using SistemaDeAnalisis.Extractors;
using SistemaDeAnalisis;
using SistemaDeAnalisis.Configuration;
using SistemaDeAnalisis.Extractors;
using SistemaDeAnalisis.Interfaces;
using SistemaDeAnalisis.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

class Program
{
    static async Task Main(string[] args)
    {
        try
        {
            Console.WriteLine(" INICIANDO DIAGNÓSTICO DEL SISTEMA ETL...");

            IHost host = Host.CreateDefaultBuilder(args)
                .ConfigureServices((context, services) =>
                {
                    Console.WriteLine(" Configurando servicios...");

                    services.Configure<ETLConfiguration>(context.Configuration.GetSection("ETL"));

                    services.AddHttpClient<ApiExtractor>();

                    services.AddSingleton<DataLoader>();

                    // === REGISTRO DE LOADERS DE DIMENSIONES ===
                    services.AddSingleton<DimCustomerLoader>(sp =>
                    {
                        var cfg = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<ETLConfiguration>>();
                        return new DimCustomerLoader(cfg.Value.ConnectionString);
                    });

                    services.AddSingleton<DimProductLoader>(sp =>
                    {
                        var cfg = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<ETLConfiguration>>();
                        return new DimProductLoader(cfg.Value.ConnectionString);
                    });

                    services.AddSingleton<DimOrderLoader>(sp =>
                    {
                        var cfg = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<ETLConfiguration>>();
                        return new DimOrderLoader(cfg.Value.ConnectionString);
                    });

                    services.AddSingleton<ExtractionService>();

                    services.AddSingleton<IExtractor, CsvExtractor>();
                    services.AddSingleton<IExtractor, DatabaseExtractor>();
                    services.AddSingleton<IExtractor, ApiExtractor>();

                    services.AddHostedService<Worker>();
                })
                .ConfigureLogging((context, logging) =>
                {
                    logging.ClearProviders();
                    logging.AddConsole();
                    logging.AddDebug();
                    logging.AddConfiguration(context.Configuration.GetSection("Logging"));
                })
                .Build();

            Console.WriteLine(" Verificando inyección de dependencias...");

            using (var scope = host.Services.CreateScope())
            {
                var serviceProvider = scope.ServiceProvider;

                try
                {
                    var extractors = serviceProvider.GetServices<IExtractor>();
                    Console.WriteLine($" Extractors encontrados: {extractors.Count()}");

                    foreach (var extractor in extractors)
                    {
                        Console.WriteLine($"   - {extractor.GetType().Name}");
                    }

                    var extractionService = serviceProvider.GetService<ExtractionService>();
                    Console.WriteLine($" ExtractionService: {(extractionService != null ? "OK" : "NULL")}");

                    var dimCustomerLoader = serviceProvider.GetService<DimCustomerLoader>();
                    var dimProductLoader = serviceProvider.GetService<DimProductLoader>();
                    var dimOrderLoader = serviceProvider.GetService<DimOrderLoader>();

                    Console.WriteLine($" DimCustomerLoader: {(dimCustomerLoader != null ? "OK" : "NULL")}");
                    Console.WriteLine($" DimProductLoader: {(dimProductLoader != null ? "OK" : "NULL")}");
                    Console.WriteLine($" DimOrderLoader: {(dimOrderLoader != null ? "OK" : "NULL")}");

                    var config = serviceProvider.GetService<Microsoft.Extensions.Options.IOptions<ETLConfiguration>>();
                    if (config?.Value != null)
                    {
                        Console.WriteLine($" Configuración ETL: OK");
                        Console.WriteLine($"   - DataDirectory: {config.Value.DataDirectory}");
                        Console.WriteLine($"   - ConnectionString: {(string.IsNullOrEmpty(config.Value.ConnectionString) ? "VACÍA" : "CONFIGURADA")}");
                    }
                    else
                    {
                        Console.WriteLine(" Configuración ETL: NULL");
                    }

                }
                catch (Exception ex)
                {
                    Console.WriteLine($" Error en inyección de dependencias: {ex.Message}");
                    Console.WriteLine($"Detalles: {ex}");
                }
            }

            Console.WriteLine(" Iniciando aplicación principal...");
            Console.WriteLine("==========================================");

            await host.RunAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine($" ERROR CRÍTICO EN LA APLICACIÓN: {ex}");
            Console.WriteLine("Presiona Enter para salir...");
            Console.ReadLine();
        }
    }
}

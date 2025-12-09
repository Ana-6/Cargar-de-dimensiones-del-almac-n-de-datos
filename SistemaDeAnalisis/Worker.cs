using SistemaDeAnalisis.Services;

namespace SistemaDeAnalisis
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly ExtractionService _extractionService;
        private readonly IHostApplicationLifetime _hostLifetime;

        public Worker(ILogger<Worker> logger, ExtractionService extractionService, IHostApplicationLifetime hostLifetime)
        {
            _logger = logger;
            _extractionService = extractionService;
            _hostLifetime = hostLifetime;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("=== INICIANDO WORKER SERVICE ===");
            _logger.LogInformation("Worker iniciado a las: {time}", DateTimeOffset.Now);

            try
            {
                await _extractionService.ExecuteExtractionAsync();

                _logger.LogInformation("=== PROCESO ETL COMPLETADO ===");

                _logger.LogInformation("Presiona Ctrl+C para salir...");

                while (!stoppingToken.IsCancellationRequested)
                {
                    await Task.Delay(1000, stoppingToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "ERROR CRÍTICO en el Worker");
                _hostLifetime.StopApplication();
            }
        }
    }
}
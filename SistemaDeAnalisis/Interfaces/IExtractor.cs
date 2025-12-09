using SistemaDeAnalisis.Models;

namespace SistemaDeAnalisis.Interfaces
{
    public interface IExtractor
    {
        Task<IEnumerable<SalesData>> ExtractAsync();
        string SourceType { get; }
    }
}
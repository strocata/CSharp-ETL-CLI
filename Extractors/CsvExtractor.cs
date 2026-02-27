using System.Globalization;
using CsvHelper;
using CsvToMsSqlCR.Mappings;
using CsvToMsSqlCR.Models;

namespace CsvToMsSqlCR.Extractors;

public interface IExtractor
{
    IAsyncEnumerable<CabTrip> ExtractAsync(string source);
}

public class CsvExtractor : IExtractor
{
    public async IAsyncEnumerable<CabTrip> ExtractAsync(string source)
    {
        using var reader = new StreamReader(source);
        using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
        
        csv.Context.RegisterClassMap<CabTripMapping>();

        var records = csv.GetRecordsAsync<CabTrip>();

        await foreach (var record in records)
        {
            yield return record; 
        }
    }
}
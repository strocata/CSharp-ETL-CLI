using Microsoft.Extensions.Configuration;
using CsvToMsSqlCR.Extractors;
using CsvToMsSqlCR.Services;

var builder = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

IConfiguration config = builder.Build();

string connectionString = config.GetConnectionString("DefaultConnection") 
                          ?? throw new InvalidOperationException("Connection string 'DefaultConnection' not found.");

string sourceCsv = "sample-cab-data.csv";
string duplicatesCsv = "duplicates.csv";

IExtractor extractor = new CsvExtractor();
var orchestrator = new CsvOrchestrator(extractor, connectionString);

Console.WriteLine("Starting ETL Pipeline...");
await orchestrator.ProcessDataAsync(sourceCsv, duplicatesCsv);
Console.WriteLine("ETL Pipeline completed successfully.");
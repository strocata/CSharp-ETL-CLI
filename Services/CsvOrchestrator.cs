using System.Data;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using CsvToMsSqlCR.Extractors;
using CsvToMsSqlCR.Models;
using Microsoft.Data.SqlClient;

namespace CsvToMsSqlCR.Services;

public class CsvOrchestrator
{
    private readonly IExtractor _extractor;
    private readonly string _connectionString;

    public CsvOrchestrator(IExtractor extractor, string connectionString)
    {
        _connectionString = connectionString;
        _extractor = extractor;
    }

    public async Task ProcessDataAsync(string sourceFilePath, string duplicatesFilePath)
    {
        var seenTrips = new HashSet<CabTripDuplicateKey>();
        var validBatch = new List<CabTrip>();
        var duplicateBatch = new List<CabTrip>();

        await foreach (var trip in _extractor.ExtractAsync(sourceFilePath))
        {
            var key = new CabTripDuplicateKey(trip.PickupDatetime, trip.DropoffDatetime, trip.PassengerCount);

            if (seenTrips.Contains(key))
            {
                duplicateBatch.Add(trip);
            }
            else
            {
                seenTrips.Add(key);
                validBatch.Add(trip);
            }

            if (validBatch.Count >= 10000)
            {
                await SaveToDatabaseAsync(validBatch);
                validBatch.Clear();
            }

            if (duplicateBatch.Count >= 10000)
            {
                await AppendToCsvAsync(duplicatesFilePath, duplicateBatch);
                duplicateBatch.Clear();
            }
        }

        if (validBatch.Any()) await SaveToDatabaseAsync(validBatch);
        if (duplicateBatch.Any()) await AppendToCsvAsync(duplicatesFilePath, duplicateBatch);
    }

    private async Task SaveToDatabaseAsync(List<CabTrip> batch)
    {

        var table = new DataTable();

        table.Columns.Add("tpep_pickup_datetime", typeof(DateTime));
        table.Columns.Add("tpep_dropoff_datetime", typeof(DateTime));
        table.Columns.Add("passenger_count", typeof(int));
        table.Columns.Add("trip_distance", typeof(decimal));
        table.Columns.Add("store_and_fwd_flag", typeof(string));
        table.Columns.Add("PULocationID", typeof(int));
        table.Columns.Add("DOLocationID", typeof(int));
        table.Columns.Add("fare_amount", typeof(decimal));
        table.Columns.Add("tip_amount", typeof(decimal));

        foreach (var trip in batch)
        {
            table.Rows.Add(
                trip.PickupDatetime,
                trip.DropoffDatetime,
                trip.PassengerCount,
                trip.TripDistance,
                trip.StoreAndFwdFlag,
                trip.PULocationId,
                trip.DOLocationId,
                trip.FareAmount,
                trip.TipAmount
            );
        }

        using (SqlConnection destinationConnection = new SqlConnection(_connectionString))
        {
            await destinationConnection.OpenAsync();

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(destinationConnection))
            {
                bulkCopy.DestinationTableName = "dbo.Trips";
                bulkCopy.BatchSize = 10000;

                try
                {
                    await bulkCopy.WriteToServerAsync(table);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Error during Bulk Insert: {ex.Message}");
                }
            }
        }
    }

    private async Task AppendToCsvAsync(string path, List<CabTrip> batch)
    {
        bool fileExists = File.Exists(path);
        using (var writer = new StreamWriter(path, append: true))
        {
            var config = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = !fileExists 
            };

            using (var csv = new CsvWriter(writer, config))
            {
                await csv.WriteRecordsAsync(batch);
            }
        }
    }
}
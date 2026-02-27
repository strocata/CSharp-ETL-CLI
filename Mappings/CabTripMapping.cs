using System.Globalization;
using CsvToMsSqlCR.Models;

namespace CsvToMsSqlCR.Mappings;

using CsvHelper.Configuration;
using System;

public class CabTripMapping : ClassMap<CabTrip>
{
    public CabTripMapping()
    {
        string[] dateFormats = { "MM/dd/yyyy hh:mm:ss tt", "M/d/yyyy h:mm:ss tt" };
        
        Map(m => m.PickupDatetime).Name("tpep_pickup_datetime").Convert(args => 
        {
            var rawDate = args.Row.GetField("tpep_pickup_datetime")!;
            
            var estTime = DateTime.ParseExact(rawDate, dateFormats, CultureInfo.InvariantCulture, DateTimeStyles.None);
            
            return TimeZoneInfo.ConvertTimeBySystemTimeZoneId(estTime, "Eastern Standard Time", "UTC");
        });

        Map(m => m.DropoffDatetime).Name("tpep_dropoff_datetime").Convert(args => 
        {
            var rawDate = args.Row.GetField("tpep_dropoff_datetime")!;
            var estTime = DateTime.ParseExact(rawDate, dateFormats, CultureInfo.InvariantCulture, DateTimeStyles.None);
            
            return TimeZoneInfo.ConvertTimeBySystemTimeZoneId(estTime, "Eastern Standard Time", "UTC");
        });

        Map(m => m.PassengerCount).Name("passenger_count").Convert(args =>
        {
            var rawText = args.Row.GetField("passenger_count");
            
            if (int.TryParse(rawText, out int parsedCount))
            {
                return parsedCount;
            }
    
            return 0; 
        });
        Map(m => m.TripDistance).Name("trip_distance");
        Map(m => m.PULocationId).Name("PULocationID");
        Map(m => m.DOLocationId).Name("DOLocationID");
        Map(m => m.FareAmount).Name("fare_amount");
        Map(m => m.TipAmount).Name("tip_amount");

        Map(m => m.StoreAndFwdFlag).Name("store_and_fwd_flag").Convert(args =>
        {
            var rawValue = args.Row.GetField("store_and_fwd_flag")?.Trim();
            
            return rawValue switch
            {
                "Y" => "Yes",
                "N" => "No",
                _ => rawValue ?? string.Empty
            };
        });
    }
}
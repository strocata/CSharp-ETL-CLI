namespace CsvToMsSqlCR.Models;

public readonly record struct CabTripDuplicateKey(
    DateTime PickupDatetime, 
    DateTime DropoffDatetime, 
    int PassengerCount
);
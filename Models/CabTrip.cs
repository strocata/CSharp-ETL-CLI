namespace CsvToMsSqlCR.Models;

public class CabTrip
{
    public DateTime PickupDatetime { get; set; }
    public DateTime DropoffDatetime { get; set; }
    public int PassengerCount { get; set; }
    public decimal TripDistance { get; set; }
    public string StoreAndFwdFlag { get; set; } = string.Empty;
    public int PULocationId { get; set; }
    public int DOLocationId { get; set; }
    public decimal FareAmount { get; set; }
    public decimal TipAmount { get; set; }
}
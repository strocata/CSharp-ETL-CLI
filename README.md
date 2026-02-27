# Cab CLI ETL Pipeline (CSV to MSSQL)

## Overview
A high-performance C# CLI application that extracts taxi trip data from a CSV, transforms it, removes duplicates, and bulk-loads it into a SQL Server database.

## Deliverables
* **Final Row Count:** 49889
* **SQL Scripts:** Located in `setup.sql` in the root directory.

## Requirement #9: Scaling to a 10GB File
Currently, deduplication is handled in-memory using a `HashSet<TripKey>`. For the provided dataset, this is highly efficient. However, for a 10GB CSV file, holding millions of keys in memory could result in an `OutOfMemoryException`.

If I knew the input was 10GB, I would shift from an ETL to an ELT (Extract, Load, Transform) approach:
1. Stream all records (including duplicates) directly into a heap-based Staging Table in SQL Server using `SqlBulkCopy`.
2. Execute a T-SQL stored procedure using `ROW_NUMBER() OVER (PARTITION BY tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count ORDER BY (SELECT NULL))` to identify duplicates.
3. Insert the unique rows into the final Production Table.
4. Export the duplicate rows back to a CSV using SQL Server tools or a lightweight C# reader.

## Assumptions Made
* **Dirty Data:** Assumed the CSV might contain missing numeric values (e.g., empty passenger counts). Implemented defensive parsing (`TryParse`) to default missing numerics to `0` rather than crashing the pipeline.
* **Database Security:** Assumed the application should not contain hardcoded credentials. Connection strings are managed via `appsettings.json`.
* **Time Zones:** The source data is in EST. Converted all datetimes to UTC before database insertion as per the "nice to have" requirement.

## How to Run
1. Execute the `setup.sql` script in your SQL Server instance to create the database, table, and optimized indexes.
2. Update the `DefaultConnection` string in `appsettings.json` with your database credentials.
3. Run the application via CLI: `dotnet run "path/to/your/sample.csv"`


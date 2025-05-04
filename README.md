# Demo_Project_

# Streaming Ingestion and CDC Tracking in Databricks

## Overview

This project demonstrates how to ingest semi-structured JSON data into Delta Lake using PySpark and simulate a Change Data Capture (CDC) pipeline by applying inserts and updates. It also generates a separate change log to track data modifications.

The project includes:
- Initial data ingestion from JSON
- CDC simulation using Delta Lake `MERGE`
- Change log generation
- Bonus: Explanation of Unity Catalog integration

## Project Structure

## Steps

1. **Load JSON Data**  
   Mock data (`mock_data.json`) is loaded into Spark DataFrame.

2. **Write to Delta Lake**  
   Data is saved to a Delta table in overwrite mode.

3. **Simulate CDC**  
   Create a second DataFrame with updates and new rows, then apply `MERGE` to upsert into the main table.

4. **Create Change Log**  
   Write all CDC events to a separate Delta table with timestamp and change type.
   

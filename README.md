# Travel Domain Data Warehouse ELT Pipeline
## Overview
This project implements a Data Warehouse and an ELT (Extract, Load, Transform) pipeline for a travel domain dataset. The pipeline is designed to process data related to flights, hotels, customers, and bookings, enabling analysis of trends in bookings and customer behavior.

The project includes:
1. Dimensional model design for the Data Warehouse.
2. ELT pipeline implementation using Python, Luigi, and dbt.
3. A snapshot pipeline to capture historical changes in dimension tables.
4. Sample queries and reports demonstrating the results of the pipeline.

## Objective
The objective of this project is to:
- Create a Data Warehouse schema for the travel domain.
- Implement an ELT pipeline to process data from the source system to the Data Warehouse.
- Enable analysis of daily booking volumes, ticket price trends, and customer behavior.

## Dataset
The dataset is based on a travel domain with multiple entities:
- Entities: Aircrafts, Airlines, Airports, Customers, Hotels, Flight      Bookings, Hotel Bookings.
- Source: PacTravel Dataset (available at Pacmann GitHub repository).

## Requirements Gathering
- Business Requirements
  - Track Daily Booking Volumes:
    Understand how many bookings are made for flights and hotels each day.
  - Monitor Average Ticket Prices Over Time:
    Analyze ticket price fluctuations to understand demand trends and inform pricing strategies.
- Proposed Solution
  - Design a Data Warehouse schema with fact tables for flight bookings, hotel bookings, and daily aggregated metrics.
  - Implement an ELT pipeline to extract raw data from the source system, load it into staging tables, and transform it into dimensional models for analysis.

## Dimensional And Fact Model Design
- Business Process
    - The business process focuses on analyzing booking trends for flights and hotels.
- Grain
  - fact_flight_bookings: One record per flight booking per customer per  seat.
  - fact_hotel_bookings: One record per hotel booking per customer.
  - fact_daily_bookings: One record per day aggregating booking volumes and average ticket prices.
- Dimensions
  - dim_customers: Contains customer details (e.g., name, gender, country).
  - dim_hotels: Contains hotel details (e.g., name, address, rating).
  - dim_airlines: Contains airline details (e.g., name, country).
  - dim_airports: Contains airport details (e.g., name, city).
  - dim_aircrafts: Contains aircraft details (e.g., model).
  - dim_date: Contains date-related attributes (e.g., year, month).
- Fact Tables
  - fact_flight_bookings: Tracks detailed flight booking information.
  - fact_hotel_bookings: Tracks detailed hotel booking information.
  - fact_daily_bookings: Aggregates daily booking volumes and average prices.
- SCD Strategy
  - Use Type 1 SCD for dimensions like dim_aircraft, dim_airline, and dim_airport (overwrite old data).
  - Use Type 2 SCD for dim_customer to track historical changes in customer attributes (customer_country) and dim_hotel to track historical changes for hotel attributes (city, country, hotel_score).
- ERD Diagram
  ![ezcv logo](https://raw.githubusercontent.com/fachrianlr/pactravel/master/assets/erd.png)


## ELT Pipeline Implementation
### Tools and Technologies
- Python: Core programming language for pipeline implementation.
- Luigi: Workflow orchestration tool for task management.
- DBT: SQL-based transformation tool for creating dimensional models and testing data quality.
- PostgreSQL: Database used for both source data and the Data Warehouse.
- Sentry: Real-time error monitoring and alerting tool.

### Pipeline Workflow
- Main Pipeline
  - Extract Data:
  Extracts raw data from the source PostgreSQL database using Python (psycopg2).
  Saves extracted data as CSV files in a structured directory (raw_data/<current_date>).
  - Load Data:
  Reads extracted CSV files using pandas.
  Loads data into staging tables in the Data Warehouse using SQLAlchemy.
  - Transform Data:
  Executes dbt commands to transform staging data into dimensional models (fact and dim tables).
  Validates transformations using dbt tests (e.g., primary key constraints).

- Snapshot Pipeline
  - Captures historical changes in dimension tables using dbt snapshots.

### Scheduling
- The pipeline can be triggered manually or scheduled using a cron job with daily schedule
- Main Pipeline Contab
```bash
0 4 * * * /opt/dataengineer/pactravel-dataset/main_pipeline.sh
```
- Snapshot Pipeline Crontab
```bash
0 5 * * * /opt/dataengineer/pactravel-dataset/snapshot_pipeline.sh
```

### Alerting
Sentry captures errors during execution and sends real-time alerts.

## Results of the Pipeline
### Sample Queries
- Fact Table: fact_daily_bookings <br>
  This table aggregates daily booking volumes and average ticket prices for flights and hotels.

```sql
SELECT 
    fact_date, 
    total_flight_bookings, 
    total_hotel_bookings, 
    avg_flight_price, 
    avg_hotel_price
FROM final.fact_daily_bookings
ORDER BY fact_date DESC
LIMIT 10;
```
- Fact Table: fact_flight_bookings <br>
This table captures detailed information about individual flight bookings.
```sql
SELECT 
    flight_number, 
    departure_date, 
    travel_class, 
    seat_number, 
    price
FROM final.fact_flight_bookings
ORDER BY departure_date DESC
LIMIT 10;
```
- Fact Table: fact_hotel_bookings <br>
This table captures detailed information about individual hotel bookings.
```sql
SELECT
    hotel_id,
    check_in_date,
    check_out_date,
    price,
    breakfast_included
FROM final.fact_hotel_bookings
ORDER BY check_in_date DESC
LIMIT 10;
```
## Conclusion
The pipeline has successfully transformed raw data into a well-designed Data Warehouse schema that meets business requirements:
- Daily booking volumes and average prices are tracked in the fact_daily_bookings table.
- Detailed booking information is available in fact_flight_bookings and fact_hotel_bookings.
- Dimensional enrichment supports advanced analysis by linking facts to descriptive attributes.

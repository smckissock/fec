## FEC Compiled Campaign Contributions

- Python scripts move [FEC data](https://www.fec.gov/data/browse-data/?tab=bulk-data) to tables in Snowflake 
- Dbt Core to setup reference data, populate a star schema, and make views for analysis
- Arrow to export views as Parquet for use on web site
- [Huey](https://github.com/rpbouman/huey) to present contribution data as pivot tables
- TODO: Add bar charts and sankey diagrams to analyse money flows by race, state, committee, etc

##### Note: this is a work in progress, so do not take the data as definitive. Instead refer to [FEC](https://www.fec.gov/) or [OpenSecrets](https://www.opensecrets.org/) 
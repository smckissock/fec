## FEC Compiled Campaign Contributions

**[Live Demo](https://smckissock.github.io/fec/)**

- Python scripts (in /import) to move [FEC data](https://www.fec.gov/data/browse-data/?tab=bulk-data) to tables in Snowflake. Largest table has individual contributions since 2000. 261 million records.  
- Dbt Core (in /transform) to setup reference data, populate a star schema, and make views for analysis
- pyarrow (in /export) to export views as Parquet files for use on web site
- [Huey](https://github.com/rpbouman/huey) (in /web) to present contribution data as pivot tables
- TODO: Add bar charts and sankey diagrams to analyse money flows by race, state, committee, etc


##### Note: this is a work in progress, so do not take the data as definitive. Instead refer to [FEC](https://www.fec.gov/) or [OpenSecrets](https://www.opensecrets.org/) 

---

### Data Pipeline

```mermaid
flowchart LR
    A[FEC Bulk Data] -->|Download & Load| B[(Snowflake Raw Tables)]
    B -->|dbt Core Transform| C[(Star Schema Tables)]
    C -->|Create Analytics Views| D[(Analytical Views)]
    D -->|pyarrow Export| E[Parquet Files]
    E -->|Huey| F[Pivot Table Visualization]
    
    subgraph Import
        A
    end
    
    subgraph Transform
        B --> C
        C --> D
    end
    
    subgraph Export & Render
        D --> E
        E --> F
    end
    
    classDef raw fill:#f9f,stroke:#333,stroke-width:2px;
    classDef transform fill:#bbf,stroke:#333,stroke-width:2px;
    classDef exportRender fill:#bfb,stroke:#333,stroke-width:2px;
    
    class A raw;
    class B raw;
    class C transform;
    class D transform;
    class E exportRender;
    class F exportRender;
```

---


![Screenshot](./screenshot.jpg)
# Personal User Acquisition Analytics Portfolio Project

A comprehensive dbt project demonstrating advanced analytics engineering skills, featuring performance optimizations, data governance, and ML-powered campaign analysis.

## Project Overview

This is a **personal portfolio project** showcasing my ability to build production-grade data pipelines for user acquisition analytics. The project processes advertising campaign data to provide insights into campaign performance, user acquisition metrics, and revenue optimization.

### Key Features Demonstrated

✅ **Advanced dbt Modeling** - Multi-layer architecture with staging, mart, and governance layers  
✅ **Performance Optimization** - Partitioning, clustering, and incremental models  
✅ **Data Governance** - Automated quality monitoring and lineage tracking  
✅ **ML Integration** - Campaign forecasting and performance classification  
✅ **Production-Ready** - Comprehensive testing and documentation  

## Technical Architecture

### Data Model Layers

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Raw Data      │ -> │   Staging       │ -> │     Mart        │
│   (Seeds)       │    │   (stg_*)       │    │   (dim_*,       │
│                 │    │                 │    │    fact_*)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
                                                        v
                                               ┌─────────────────┐
                                               │   Governance    │
                                               │   (governance_*)│
                                               └─────────────────┘
```

### Models Overview

#### Staging Layer (Raw → Clean)
- `stg_spend` - Advertising spend data (incremental)
- `stg_user_data` - User acquisition data (incremental)  
- `stg_campaign_metrics` - Campaign metrics from ML analysis
- `stg_dim_cv_bucket` - Conversion value bucket mapping

#### Mart Layer (Business Logic)
**Dimensions:**
- `dim_campaign` - Campaign dimension with hash-based surrogate keys
- `dim_network` - Network dimension
- `dim_platform` - Platform dimension (android/ios)
- `dim_date` - Date dimension with time attributes
- `dim_campaign_performance_tier` - Campaign performance classification

**Facts:**
- `fact_campaign_performance` - Campaign performance metrics
- `fact_campaign_daily` - Daily performance (partitioned by date, clustered)
- `fact_campaign_summary` - Aggregated performance summary

#### Governance Layer (Data Quality)
- `governance_data_quality` - Data quality monitoring and scoring
- `governance_data_lineage` - Data lineage documentation

## Performance Optimizations Implemented

### 1. Partitioning & Clustering
```sql
-- fact_campaign_daily is partitioned by date and clustered by key dimensions
partition_by:
  field: date
  data_type: date
  granularity: day
cluster_by: ["platform_id", "network_id", "campaign_id"]
```

### 2. Incremental Models
- `stg_spend` and `stg_user_data` use incremental materialization
- Only processes new data since last run
- Reduces processing time and costs

### 3. Materialized Summary Tables
- `fact_campaign_summary` provides pre-aggregated metrics
- Faster queries for dashboard and reporting

### 4. Hash-Based Surrogate Keys
- Uses `dbt_utils.generate_surrogate_key()` for consistent keys
- Better performance than row_number() for large datasets

## Data Governance Features

### Data Quality Framework
- Automated data quality checks for all tables
- Quality scoring (0-100) for each data source
- Status classification: Excellent, Good, Fair, Poor

### Data Lineage
- Complete documentation of data flow
- Ownership and classification for each table
- Retention policies and refresh frequencies

### Compliance Features
- PII/Financial data classification
- GDPR-ready tracking status handling
- iOS opt-out user revenue estimation

## Getting Started

### Prerequisites
- dbt Core or dbt Cloud
- BigQuery (or compatible data warehouse)
- Python 3.8+

### Installation

1. Install dbt packages:
```bash
dbt deps
```

2. Configure your profile in `profiles.yml`:
```yaml
voodoo_case:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: your-project-id
      dataset: your_dataset
      keyfile: path/to/keyfile.json
```

3. Run the models:
```bash
dbt run
```

4. Run tests:
```bash
dbt test
```

### Incremental Runs
For production, use incremental runs:
```bash
dbt run --models +fact_campaign_daily
```

## Data Quality Monitoring

### Quality Checks
The project includes automated quality checks for:
- **Data Completeness**: Null values in critical fields
- **Data Consistency**: Valid values for categorical fields
- **Referential Integrity**: Foreign key relationships
- **Business Logic**: ROAS, revenue, and spend validation

### Monitoring Dashboard
Query the governance tables for monitoring:
```sql
SELECT * FROM governance_data_quality 
ORDER BY created_at DESC;
```

## Performance Best Practices

### Query Optimization
1. Always filter by date when querying `fact_campaign_daily`
2. Use `fact_campaign_summary` for high-level metrics
3. Join on surrogate keys for optimal performance

### Example Queries

#### Daily Performance by Platform
```sql
SELECT 
    p.platform,
    d.date,
    SUM(cd.daily_revenue) as revenue,
    SUM(cd.daily_spend) as spend,
    AVG(cd.daily_roas) as avg_roas
FROM fact_campaign_daily cd
JOIN dim_platform p ON cd.platform_id = p.platform_id
JOIN dim_date d ON cd.date_id = d.date_id
WHERE d.date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY p.platform, d.date
ORDER BY d.date DESC;
```

#### Campaign Health Score
```sql
SELECT 
    c.campaign_name,
    cs.campaign_health_score,
    cs.recommended_action
FROM fact_campaign_summary cs
JOIN dim_campaign c ON cs.campaign_id = c.campaign_id
ORDER BY cs.campaign_health_score DESC;
```

## Project Structure

```
.
├── models/
│   ├── staging/           # Raw data transformation
│   ├── mart/              # Business logic
│   │   ├── dimensions/    # Dimension tables
│   │   └── facts/         # Fact tables
│   └── governance/        # Data quality & lineage
├── seeds/                 # Static reference data
├── tests/                 # Data quality tests
├── notebook/              # ML analysis notebooks
├── dbt_project.yml        # dbt configuration
├── dbt_packages.yml       # Package dependencies
└── README.md              # This file
```

## Technologies Used

- **dbt Core** - Data transformation and modeling
- **BigQuery** - Data warehouse
- **Python** - ML analysis and forecasting
- **dbt_utils** - Utility macros and functions
- **dbt_date** - Date handling utilities

## What This Project Demonstrates

### Technical Skills
- ✅ Advanced dbt modeling patterns
- ✅ Performance optimization techniques
- ✅ Data governance implementation
- ✅ ML integration with analytics
- ✅ Production-ready code quality

### Business Acumen
- ✅ User acquisition analytics
- ✅ Campaign performance optimization
- ✅ Revenue attribution
- ✅ Data-driven decision making

### Professional Practices
- ✅ Comprehensive documentation
- ✅ Automated testing
- ✅ Code organization
- ✅ Version control best practices

## About This Project

This is a **personal portfolio project** created to demonstrate advanced analytics engineering capabilities. It showcases my ability to:

1. Design and implement complex data models
2. Optimize performance for large datasets
3. Implement data governance frameworks
4. Integrate ML models with analytics pipelines
5. Build production-ready data solutions

The project uses simulated advertising campaign data to demonstrate real-world analytics scenarios that businesses face when managing user acquisition across multiple platforms and networks.

## Contributing

This is a personal portfolio project, but I welcome feedback and suggestions for improvement. Feel free to:
- Open issues for bugs or suggestions
- Fork and experiment with the code
- Use it as a learning resource

## License

This project is open source and available for educational purposes.

## Connect

- **GitHub**: [\[Your GitHub Profile\]](https://github.com/fefecoding)
- **LinkedIn**: [\[Your LinkedIn Profile\]](https://www.linkedin.com/in/trannammai/)

---

*Built with ❤️ by [Your Name] - Analytics Engineer*
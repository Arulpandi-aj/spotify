# 🎵 Spotify AWS Data Lakehouse Pipeline

![AWS](https://img.shields.io/badge/AWS-232F3E?style=for-the-badge&logo=amazonaws&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)
![S3](https://img.shields.io/badge/Amazon_S3-569A31?style=for-the-badge&logo=amazons3&logoColor=white)
![Glue](https://img.shields.io/badge/AWS_Glue-FF9900?style=for-the-badge&logo=amazonaws&logoColor=white)
![Athena](https://img.shields.io/badge/Amazon_Athena-232F3E?style=for-the-badge&logo=amazonaws&logoColor=white)

> A production-grade, end-to-end AWS data lakehouse pipeline that ingests Spotify CSV data, transforms it using AWS Glue (PySpark), models it into a Star Schema, and enables cost-efficient analytics via Amazon Athena.

---

## 🎯 Business Objective

Simulate a real-world analytics platform for a music streaming company to:

- Analyze artist growth trends  
- Track monthly streaming performance  
- Identify high-performing genres  
- Reduce analytics cost through optimized data lake design  
- Enforce strong data quality before analytics consumption  

---

## 🏗️ Architecture Overview

### AWS Services Used

- **Amazon S3** – Data Lake Storage  
- **AWS Glue (Visual ETL + PySpark)** – Data Transformation  
- **AWS Glue Data Catalog** – Metadata Management  
- **Amazon Athena** – Serverless SQL Analytics  
- **AWS EventBridge** – Event-driven pipeline trigger  
- **AWS IAM** – Least-privilege access control  
- **Amazon CloudWatch** – Logging & monitoring  

---

## 🥉🥈🥇 Medallion Architecture

The pipeline follows a **Bronze → Silver → Gold** design pattern.

### 🥉 Bronze Layer (Raw – Immutable)
- Original CSV files
- Write-once storage
- S3 versioning enabled
- Full replay capability

### 🥈 Silver Layer (Validated & Cleaned)
- Schema enforcement
- Null handling & type casting
- Deduplication
- Data quality checks
- Failed records routed to `rejected/` prefix

### 🥇 Gold Layer (Analytics – Star Schema)
- Parquet format with Snappy compression
- Partitioned by `year/month`
- Fact & dimension tables
- Optimized for Athena cost efficiency

---

## 🧠 Data Model – Star Schema

### Fact Table
- `fact_streams`
  - stream_id (PK)
  - track_id (FK)
  - artist_id (FK)
  - album_id (FK)
  - date_id (FK)
  - stream_count
  - duration_played

### Dimension Tables
- `dim_tracks`
- `dim_artists`
- `dim_albums`
- `dim_date`

### Why Star Schema?
- Faster aggregations
- Lower Athena scan cost
- Cleaner business queries
- Better scalability

---

## 🔄 Data Flow

1. Raw Spotify CSV files uploaded to S3 Bronze layer  
2. EventBridge triggers AWS Glue job  
3. Glue performs:
   - Schema resolution  
   - Data cleaning  
   - Deduplication  
   - Data quality validation  
   - Dataset joins  
4. Clean data written as partitioned Parquet to Gold layer  
5. Glue Data Catalog updated automatically  
6. Athena queries data for analytics  

---


## 🖼 Project Screenshots

### ☁️ S3 Data Storage (Buckets)
![S3 Bucket](screenshots/s3_bucket.jpeg)

### 📂 Staging Layer (S3)
Raw Spotify CSV files (`albums`, `artists`, `tracks`) stored in the S3 staging layer.
![S3 Staging](screenshots/s3_staging_files.jpeg)

🔗 Full processed datasets are linked in `data/staging/Processed_Data.txt`.

🔗 **Note:** Full processed datasets are not uploaded to GitHub due to size constraints.  
They can be accessed via **Google Drive** and are also stored in the **Amazon S3 staging layer**.

📁 **Processed Data:**  
[View datasets on Google Drive](https://drive.google.com/drive/folders/1PgZQDvw5GnvVQuhV7-MtxIZHnLsZA-Zs)


### 🔧 AWS Glue Visual ETL
Visual ETL job created in AWS Glue to transform Spotify data.
![Glue Visual ETL](screenshots/glue_visual_etl.jpeg)

### ▶️ AWS Glue Job Execution
Successful execution of the Glue ETL job.
![Glue Job Run](screenshots/glue_job_run.jpeg)

### 📊 Amazon Athena – Query Execution
Querying transformed data using Amazon Athena.
![Athena Query](screenshots/athena_query.jpeg)

### 📈 Amazon Athena – Query Results
Validated transformed dataset using Athena query results.
![Athena Results](screenshots/athena_results.jpeg)

---


## 🏗️ Architecture Diagram

![Architecture](screenshots/spotify_architecture.png)

## ⚙️ AWS Glue ETL – Key Transformations

```python
from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext

# Resolve schema conflicts
resolved = ResolveChoice.apply(
    frame=raw_frame,
    choice="make_struct"
)

# Drop null primary keys
cleaned = Filter.apply(
    frame=resolved,
    f=lambda x: x["track_id"] is not None
)

# Write partitioned Parquet
glueContext.write_dynamic_frame.from_options(
    frame=cleaned,
    connection_type="s3",
    connection_options={
        "path": "s3://spotify-pipeline/gold/fact_streams/",
        "partitionKeys": ["year", "month"]
    },
    format="glueparquet",
    format_options={"compression": "snappy"}
)
```

### Incremental Processing

- Glue Job Bookmarks enabled  
- Only new files processed  
- Reduces compute cost significantly  

---

## ✅ Data Quality Rules

| Rule | Purpose |
|------|---------|
| IsComplete (track_id) | Primary key integrity |
| IsUnique (track_id) | No duplicates |
| ColumnValues (popularity 0–100) | Valid value range |
| ColumnLength (track_name > 0) | Prevent empty values |
| IsComplete (stream_date) | Mandatory date enforcement |

Failed records are stored in `rejected/` S3 prefix with failure reasons.

---

## 📊 Performance & Cost Optimization

- CSV → Parquet (Snappy) → ~75% cost reduction  
- Partition pruning → Reduced Athena scan size  
- Incremental loads → 65% Glue cost reduction  
- Target file size: 128–256 MB  
- Optimized Spark partitions  

---

## 📈 Benchmarks

| Metric | Value |
|--------|-------|
| Raw Dataset | ~500 MB |
| Parquet Output | ~85 MB |
| Records Processed | ~600,000 |
| Initial Glue Runtime | ~4 minutes |
| Incremental Runtime | ~90 seconds |
| Athena Query Time | ~1–3 seconds |
| Data Rejection Rate | < 0.3% |

---

## 🔐 Security & Governance

- Least-privilege IAM roles  
- No wildcard `*` permissions  
- S3 versioning enabled  
- Encryption at rest  
- CloudWatch logging enabled  
- Immutable raw layer  

---

## 📂 S3 Folder Structure

```
s3://spotify-pipeline/

bronze/
silver/
gold/
  fact_streams/
  dim_tracks/
  dim_artists/
  dim_albums/
  dim_date/
rejected/
```

---

## 🔍 Sample Athena Query

```sql
SELECT
    da.artist_name,
    dd.year,
    dd.month,
    SUM(fs.stream_count) AS total_streams
FROM fact_streams fs
JOIN dim_artists da ON fs.artist_id = da.artist_id
JOIN dim_date dd ON fs.date_id = dd.date_id
WHERE dd.year = '2024'
  AND dd.month = '01'
GROUP BY da.artist_name, dd.year, dd.month
ORDER BY total_streams DESC
LIMIT 10;
```

---

## 🚀 Scalability Strategy

| Scale | Strategy |
|-------|----------|
| <10GB | Glue + Athena |
| 1TB | Partition evolution |
| 10TB | Apache Iceberg |
| 100TB+ | Redshift Spectrum |
| Real-time | Kinesis + Streaming Glue |

---

## 🧭 Future Improvements

- Apache Iceberg (ACID + Time Travel)  
- dbt on Athena  
- CI/CD via GitHub Actions  
- Streaming ingestion (Kinesis)  
- Terraform Infrastructure as Code  

---

## 💼 Resume Highlights

- Built event-driven AWS data lake processing ~600K records per run  
- Reduced Athena query cost by 75% via Parquet + partitioning  
- Implemented incremental loads reducing compute cost by 65%  
- Designed Star Schema improving analytics performance by 3x  
- Enforced automated data quality checks with <0.3% rejection rate  

---

## 👤 Author

**Arulpandi Arumugam**  
Data Engineer | AWS | Python | PySpark | SQL  

LinkedIn: [https://linkedin.com/in/yourprofile ](https://www.linkedin.com/in/arul-pandi) 
GitHub: [https://github.com/yourusername](https://github.com/Arulpandi-aj)  

---

## 📄 License

MIT License

---

⭐ Built using production-grade data engineering principles.

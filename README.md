# Real-Time Customer Data Lakehouse

Enterprise-grade real-time customer data lakehouse built using Azure Databricks, Delta Live Tables, Delta Lake, and Medallion Architecture.

## Tech Stack

- Python\
- Azure Event Hub\
- Azure Data Lake Storage Gen2 (ADLS)\
- Azure Databricks\
- Delta Live Tables (DLT)\
- Delta Lake\
- PySpark\
- Power BI\

# Architecture Overview

 <img width="1407" height="768" alt="1779462501177" src="https://github.com/user-attachments/assets/7223aef7-5267-49be-9833-960104ecb62b" />

This project follows the Medallion Architecture approach:

## ➡️ Data Generation & Ingestion

▫️Python-based JSON data generator\
▫️Real-time ingestion through Azure Event Hub\
▫️Structured Streaming using Databricks\

## ➡️ Bronze Layer

▫️Raw immutable JSON storage\
▫️Historical event retention in ADLS Gen2\
▫️Acts as source of truth 

## ➡️ Silver Layer

▫️Parsing json data and cleansing\
▫️Schema enforcement using Autoloader\
▫️Handling nulls and missing data\
▫️Deduplication 
▫️Writing data in delta format \

## ➡️ Gold Layer

▫️Business-ready fact and dimension tables\
▫️Aggregated customer analytics\
▫️Serving & Analytics\
▫️Power BI dashboards\
▫️Analytical reporting\

## ⭐ Key Features ⭐

▫️Real-time streaming pipeline\
▫️End-to-end Medallion Architecture\
▫️Incremental data processing\
▫️SCD Type 1 & Type 2 implementation\
▫️Cloud-native scalable design\

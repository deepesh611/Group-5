# Natural Language Data Explorer & Self-Healing Pipeline

## Overview
This project provides an end-to-end cloud data pipeline and AI-driven data exploration tool built on Azure Databricks. It ingests synthetic healthcare data (Synthea), properly handles schema drift and PHI masking, and exposes a Natural Language Text-to-SQL interface powered by LangChain and OpenAI. This allows business and clinical users to query complex medical data using plain English.

## Features
- **Data Ingestion & Self-Healing Pipeline**: Converts raw healthcare CSVs to Delta tables. Automatically cleans column names, infers data types, detects schema drift, and applies dynamic PHI masking (hashing, redacting, generalizing).
- **Multi-Agent Text-to-SQL System**: A complex pipeline using LangGraph, LangChain, and OpenAI to convert natural language queries into executable Spark SQL against Databricks Unity Catalog. Includes specialized agents for data intent classification, schema fetching, SQL generation, and auto-correction (SQL Fix Agent).
- **Streamlit Data Explorer**: A chat-based Databricks App UI allowing users to easily interact with the AI pipeline. 

## Project Structure
- `ai-data-explorer/`: Contains the frontend Streamlit application which exposes the multi-agent text-to-SQL chatbot.
- `Text-to-SQL LLM/`: Jupyter notebooks detailing the research, prototyping, and testing of the LangChain/LangGraph pipelines.
- `csv_to_delta.py` & `csv_to_delta_databricks_azure.py`: The ingestion scripts handling CSV parsing, data transformations, PHI masking, and writing to Delta Tables.
- `master_schema.json`: The canonical schema definition used for drift detection and table validation.
- `DATABRICKS_SETUP.md`: Comprehensive guide for provisioning and deploying the Azure and Databricks infrastructure.

## Getting Started
To deploy this project to Azure Databricks, please follow the detailed steps in the [Databricks Setup Guide](DATABRICKS_SETUP.md). It will walk you through:
1. Setting up an Azure Resource Group and ADLS Gen2.
2. Configuring the Databricks Workspace (Compute, Libraries, Unity Catalog).
3. Establishing Secret Scopes for Security (OpenAI keys, ADLS keys).
4. Running the ingestion pipelines and AI apps.

## Tech Stack
- **Cloud Platform:** Microsoft Azure
- **Data Platform:** Azure Databricks, Databricks SQL Connector, Unity Catalog
- **Storage:** Azure Data Lake Storage Gen2 (ADLS Gen2), Delta Lake
- **AI/LLM:** OpenAI GPT-4, LangChain, LangGraph, Pydantic
- **Frontend UI:** Streamlit (Databricks Apps)
- **Data Processing:** PySpark
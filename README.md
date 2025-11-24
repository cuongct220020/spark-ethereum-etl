# Ethereum ETL Pipeline with Apache Spark

This repository processes Ethereum blockchain data using Apache Spark. It transforms raw blockchain data into structured fact and dimension tables for analytics.

## Workflow Overview

The pipeline follows these steps:

1. Extract Ethereum blockchain data using eth-etl tools
2. Organize data into CSV files in the `data/` directory
3. Process data using Spark to create structured fact and dimension tables
4. Output results as CSV files in the `output/` directory

## Prerequisites

- Python 3.7+
- Docker and Docker Compose
- Ethereum blockchain data in CSV format
- eth-etl tools installed (for data extraction)

Install required Python packages:
```bash
pip install -r requirements.txt
```

## Step-by-Step Process

### 1. Data Extraction

First, get the block range for a specific date:
```bash
get_block_range --start-date YYYY-MM-DD --end-date YYYY-MM-DD
```

For example, to extract 100 blocks in the range [20659158, 20659258]:
```bash
# Create data directory
mkdir -p data

# Grant execution permission
chmod +x ./scripts/export_blocks_transactions

# Extract transactions for the block range
./scripts/export_blocks_transactions 20659158 20659258
```

### 2. Extract Additional Data Types

Extract supporting data for the same block range:
```bash
# Grant execution permission
chmod +x ./scripts/export_receipts_logs
chmod +x ./scripts/export_token_transfers
chmod +x ./scripts/export_contracts
chmod +x ./scripts/export_tokens

# Extract transaction receipts
./scripts/export_receipts_logs 20659158 20659258

# Extract token transfers
./scripts/export_token_transfers 20659158 20659258

# Extract contract information
./scripts/export_contracts 20659158 20659258

# Extract token information
./scripts/export_tokens 20659158 20659258
```

### 3. Prepare Environment

Organize extracted data and create required directories:
```bash
# Ensure data directory exists and contains CSV files
ls -la data/

# Create output directory
mkdir -p output

# Create Spark event log directory
mkdir -p spark-events
```

### 4. Run Spark ETL Job

Start the Spark cluster and execute the ETL job:
```bash
# Launch Spark cluster in detached mode
docker compose up -d

# Submit the ETL job (this will process data in data/ and output to output/)
docker compose up spark-client
```

### 5. Check Results

After the job completes, examine the output:
```bash
# Check the processed output
ls -la output/native_transactions/
ls -la output/token_transactions/
```

The output contains two main datasets:
- `native_transactions`: Ethereum native transaction details
- `token_transactions`: Token transfer information

## Architecture

The pipeline uses a Spark cluster with:
- Master node for coordinating jobs
- Worker nodes for data processing
- History server for monitoring
- Client container for submitting the ETL job

The data flows from the `data/` directory through the Spark cluster, undergoing transformations to produce structured CSV output in the `output/` directory.
"""
Ethereum ETL Job - Processes Ethereum blockchain data into fact and dimension tables.

This script reads raw Ethereum blockchain data from CSV files and processes them
into two main outputs:
1. Native transactions (fact_native_transactions)
2. Token transfers (fact_token_transfers)

The job is designed to run in a Spark cluster environment configured via docker-compose.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, pow, to_timestamp, regexp_replace, substring, isnan, coalesce
)
from pyspark.sql.types import (
    StringType, LongType, DecimalType, IntegerType, DoubleType
)
import os
import sys
import logging
from typing import Optional


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Define data types and constants
class Constants:
    """Constants used across the ETL job."""
    DECIMAL_38_0 = DecimalType(38, 0)
    DECIMAL_38_18 = DecimalType(38, 18)
    WEI_TO_ETHER = 1000000000000000000.0


def create_spark_session(app_name: str = "EthereumETLJob") -> SparkSession:
    """Create and configure Spark session for the ETL job."""
    logger.info(f"Creating Spark session: {app_name}")
    
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.ansi.enabled", "false") \
        .getOrCreate()
    
    logger.info("Spark session created successfully")
    return spark


def read_input_data(spark: SparkSession, input_data_path: str) -> dict:
    """Read all input CSV files into DataFrames."""
    logger.info(f"Reading input data from: {input_data_path}")
    
    # Define the input files to read
    input_files = {
        'transactions': 'transactions_*.csv',
        'receipts': 'receipts_*.csv', 
        'tokens': 'tokens_*.csv',
        'token_transfers': 'token_transfers_*.csv',
        'contracts': 'contracts_*.csv'
    }
    
    dataframes = {}
    
    for table_name, file_pattern in input_files.items():
        file_path = os.path.join(input_data_path, file_pattern)
        logger.info(f"Reading {table_name} from: {file_path}")
        
        try:
            df = spark.read.csv(file_path, header=True, inferSchema=True)
            logger.info(f"Successfully read {table_name} with {df.count()} records")
            dataframes[table_name] = df
        except Exception as e:
            logger.error(f"Failed to read {table_name} from {file_path}: {str(e)}")
            raise
    
    logger.info(f"Successfully read all {len(dataframes)} input tables")
    return dataframes


def clean_tokens_data(tokens_df):
    """Clean and process the tokens dimension table."""
    logger.info("Starting token data cleaning process")
    
    # Clean tokens data by handling nulls and invalid values
    cleaned_df = tokens_df.alias("t")
    
    # Handle decimal values: use provided value or default to 18
    cleaned_df = cleaned_df.withColumn(
        "token_decimals",
        coalesce(col("decimals"), lit(18)).cast(IntegerType())
    )
    
    # Handle symbol: use provided value or default to '[UNKNOWN]'
    cleaned_df = cleaned_df.withColumn(
        "token_symbol",
        coalesce(col("symbol"), lit("[UNKNOWN]")).cast(StringType())
    )
    
    # Handle name: use provided value or default to '[UNKNOWN_NAME]'
    cleaned_df = cleaned_df.withColumn(
        "token_name",
        coalesce(col("name"), lit("[UNKNOWN_NAME]")).cast(StringType())
    )
    
    # Filter out rows where total_supply is NaN or Infinity
    cleaned_df = cleaned_df.filter(
        (~isnan(col("total_supply"))) & (col("total_supply").isNotNull())
    )
    
    # Select and rename final columns
    result_df = cleaned_df.select(
        col("address").alias("token_address"),
        col("token_symbol"), 
        col("token_name"),
        col("token_decimals")
    )
    
    logger.info(f"Token data cleaning completed. Final count: {result_df.count()}")
    return result_df


def process_native_transactions(transactions_df, receipts_df):
    """Process native Ethereum transactions with receipts."""
    logger.info("Starting native transaction processing")
    
    # Define columns to exclude from transactions
    cols_to_drop_from_tx = [
        "transaction_index", 
        "max_fee_per_gas", 
        "max_priority_fee_per_gas", 
        "max_fee_per_blob_gas",
        "blob_versioned_hashes"
    ]
    
    # Build column list excluding dropped columns
    tx_all_cols = [
        col(f"tx.{c}") for c in transactions_df.columns if c not in cols_to_drop_from_tx
    ]
    
    # Add receipt columns
    rcpt_cols = [
        col("rcpt.status").alias("tx_status"),
        col("rcpt.gas_used"),
    ]
    
    # Join transactions with receipts
    tx_receipt_df = transactions_df.alias("tx").join(
        receipts_df.alias("rcpt"),
        on=col("tx.hash") == col("rcpt.transaction_hash"), 
        how='inner'
    ).select(
        *tx_all_cols,
        *rcpt_cols
    )
    
    # Rename columns for clarity
    renamed_df = tx_receipt_df \
        .withColumnRenamed("hash", "tx_hash") \
        .withColumnRenamed("nonce", "tx_nonce") \
        .withColumnRenamed("from_address", "sender_address") \
        .withColumnRenamed("to_address", "receiver_address") \
        .withColumnRenamed("value", "value_wei") \
        .withColumnRenamed("gas_price", "gas_price_wei") \
        .withColumnRenamed("gas", "gas_limit") \
        .withColumnRenamed("transaction_type", "tx_type") \
        .withColumnRenamed("input", "tx_input") \
        .withColumnRenamed("block_timestamp", "block_timestamp_unix")
    
    # Apply transformations to create final native transactions table
    native_tx_df = renamed_df.select(
        # Key and block information
        col("tx_hash"),
        col("block_number").cast(LongType()),
        to_timestamp(col("block_timestamp_unix")).alias("block_timestamp"),
        col("block_hash"),
        
        # Sender/receiver addresses
        col("sender_address"),
        col("receiver_address"),

        # Value conversion from wei to eth
        (col("value_wei") / lit(Constants.WEI_TO_ETHER)).alias("value_eth").cast(Constants.DECIMAL_38_18),

        # Transaction status and type
        col("tx_status").cast(IntegerType()),
        col("tx_type").cast(IntegerType()),

        # Gas information
        col("gas_price_wei").alias("gas_price").cast(LongType()),
        col("gas_used").cast(LongType()),

        # Fee calculation
        ((col("gas_used") * col("gas_price_wei")) / lit(Constants.WEI_TO_ETHER))
        .alias("tx_fee_eth").cast(Constants.DECIMAL_38_18),

        # Method ID extraction
        substring(regexp_replace(col("tx_input"), "0x", ""), 1, 8).alias("tx_input_method_id"),

        # Nonce
        col("tx_nonce").cast(LongType()),
    )
    
    logger.info(f"Native transaction processing completed. Final count: {native_tx_df.count()}")
    return native_tx_df


def process_token_transfers(token_transfers_df, cleaned_tokens_df, contracts_df, native_tx_df):
    """Process token transfers with token and contract information."""
    logger.info("Starting token transfer processing")
    
    # Join token transfers with tokens and contracts
    token_data_df = token_transfers_df.alias("tt").join(
        cleaned_tokens_df.alias("t"),
        on=col("tt.token_address") == col("t.token_address"),
        how="left"
    ).join(
        contracts_df.alias("c"),
        on=col("tt.token_address") == col("c.address"),
        how="left"
    ).select(
        # Transaction and log info
        col("tt.transaction_hash").alias("tx_hash"),
        col("tt.log_index").cast(LongType()),
        col("tt.block_number").cast(LongType()),
        
        # Sender/receiver info
        col("tt.from_address").alias("sender_address"),
        col("tt.to_address").alias("receiver_address"),
        
        # Token info
        col("tt.token_address"),
        col("t.token_symbol"),
        col("t.token_decimals").cast(IntegerType()),
        
        # Raw amount (as string to handle large values)
        col("tt.value").cast(StringType()).alias("amount_raw"), 
        
        # Token type determination
        when(col("c.is_erc20") == True, lit("ERC20"))
        .when(col("c.is_erc721") == True, lit("ERC721"))
        .otherwise(lit("OTHER")).alias("token_type")
    )
    
    # Join with native transactions to get block timestamp
    df_temp_join = token_data_df.alias("td").join(
        native_tx_df.alias("nt"),
        on=col("td.tx_hash") == col("nt.tx_hash"),
        how="inner"
    )
    
    # Final processing with calculations
    token_tx_df = df_temp_join.select(
        # Transaction identifiers
        col("td.tx_hash"),
        col("td.log_index"),
        
        # Block information
        col("nt.block_number"),
        col("nt.block_timestamp"),
        col("nt.tx_nonce"),
        
        # Token information
        col("td.token_address"),
        col("td.token_symbol"),
        col("td.token_type"),
        
        # Transfer information
        col("td.sender_address"),
        col("td.receiver_address"),
        
        # Amount calculations
        col("td.amount_raw").cast(Constants.DECIMAL_38_0).alias("amount_raw"),
        col("td.token_decimals"),

        # Calculate display amount
        (col("td.amount_raw").cast(DoubleType()) / (pow(lit(10.0), col("td.token_decimals"))))
        .alias("amount_display").cast(Constants.DECIMAL_38_18)
    ).select(
        # Reorder columns to standard format
        "tx_hash", "log_index", "block_number", "block_timestamp", "tx_nonce",
        "token_address", "token_symbol", "token_type", 
        "sender_address", "receiver_address",
        "amount_raw", "amount_display", "token_decimals"
    )
    
    logger.info(f"Token transfer processing completed. Final count: {token_tx_df.count()}")
    return token_tx_df


def write_output_data(native_tx_df, token_tx_df, output_data_path: str):
    """Write processed DataFrames to output CSV files."""
    logger.info(f"Writing output data to: {output_data_path}")
    
    # Define output paths
    native_tx_output_path = os.path.join(output_data_path, 'native_transactions')
    token_tx_output_path = os.path.join(output_data_path, 'token_transactions')
    
    logger.info(f"Writing native transactions to: {native_tx_output_path}")
    native_tx_df.repartition(1).write.csv(
        path=native_tx_output_path, 
        mode="overwrite", 
        header=True, 
        sep=","
    )
    logger.info("Native transactions written successfully")
    
    logger.info(f"Writing token transactions to: {token_tx_output_path}")
    token_tx_df.repartition(1).write.csv(
        path=token_tx_output_path, 
        mode="overwrite", 
        header=True, 
        sep=","
    )
    logger.info("Token transactions written successfully")
    
    logger.info("All output data written successfully")


def run_full_etl(input_data_path: str, output_data_path: str):
    """Main ETL function that orchestrates the entire process."""
    logger.info(f"Starting ETL job with input: {input_data_path}, output: {output_data_path}")
    
    spark = create_spark_session("EthereumETLJob")
    
    try:
        # Read input data
        dataframes = read_input_data(spark, input_data_path)
        
        # Process tokens data (dimension table)
        cleaned_tokens_df = clean_tokens_data(dataframes['tokens'])
        
        # Process native transactions (fact table)
        native_tx_df = process_native_transactions(
            dataframes['transactions'], 
            dataframes['receipts']
        )
        
        # Process token transfers (fact table) 
        token_tx_df = process_token_transfers(
            dataframes['token_transfers'],
            cleaned_tokens_df,
            dataframes['contracts'],
            native_tx_df
        )
        
        # Write output data
        write_output_data(native_tx_df, token_tx_df, output_data_path)
        
        logger.info("ETL job completed successfully")
        
    except Exception as e:
        logger.error(f"ETL job failed with error: {str(e)}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")


def get_default_paths() -> tuple:
    """Get default input and output paths based on script location."""
    base_dir = os.path.dirname(os.path.abspath(__file__))
    input_default = os.path.join(os.path.dirname(base_dir), 'data')
    output_default = os.path.join(os.path.dirname(base_dir), 'output')
    return input_default, output_default


if __name__ == "__main__":
    if len(sys.argv) < 3:
        # Use default paths if no arguments provided
        input_path, output_path = get_default_paths()
        logger.info(f"No arguments provided, using default paths: input={input_path}, output={output_path}")
    else:
        input_path = sys.argv[1]
        output_path = sys.argv[2]
        logger.info(f"Using provided paths: input={input_path}, output={output_path}")
    
    run_full_etl(input_path, output_path)
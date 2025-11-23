from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType, TimestampType, IntegerType

# ==============================================================================
# 1. SCHEMA: FACT_NATIVE_TRANSACTIONS
# Dùng cho các giao dịch chuyển ETH và thông tin kỹ thuật của Transaction
# ==============================================================================
native_tx_schema = StructType([
    # Khóa chính và thông tin khối
    StructField("tx_hash", StringType(), False),  # Mã băm giao dịch (Primary Key)
    StructField("block_number", LongType(), False),  # Số khối
    StructField("block_timestamp", TimestampType(), False),  # Thời gian thực hiện

    # Thông tin người gửi/nhận và giá trị
    StructField("from_address", StringType(), True),  # Ví người gửi (Người trả phí Gas)
    StructField("to_address", StringType(), True),  # Ví người nhận (hoặc Contract Address)
    StructField("value_eth", DecimalType(38, 18), True),  # Số lượng ETH chuyển đi (Đơn vị: Ether)

    # Thông tin về phí và trạng thái kỹ thuật
    StructField("gas_price", LongType(), True),  # Đơn giá Gas (Wei)
    StructField("gas_used", LongType(), True),  # Lượng Gas tiêu thụ thực tế
    StructField("tx_fee_eth", DecimalType(38, 18), True),  # Tổng phí transaction (Ether)
    StructField("status", IntegerType(), True),  # 1 = Success, 0 = Fail

    # Metadata bổ sung
    StructField("input_method_id", StringType(), True),  # 8 ký tự đầu của input data (để biết gọi hàm gì)
    StructField("nonce", LongType(), True)  # Số thứ tự giao dịch của ví gửi
])

# ==============================================================================
# 2. SCHEMA: FACT_TOKEN_TRANSFERS
# Dùng cho các giao dịch chuyển Token (ERC20) và NFT (ERC721)
# ==============================================================================
token_transfer_schema = StructType([
    # Khóa ngoại liên kết với bảng Native
    StructField("tx_hash", StringType(), False),  # Link tới bảng Native Transaction
    StructField("log_index", LongType(), False),  # Thứ tự sự kiện trong log (để phân biệt trong 1 tx)

    # Thông tin thời gian (Duplicate từ Native để tiện truy vấn nhanh mà không cần join)
    StructField("block_number", LongType(), False),
    StructField("block_timestamp", TimestampType(), False),

    # Thông tin Token
    StructField("token_address", StringType(), True),  # Địa chỉ Smart Contract của Token
    StructField("token_symbol", StringType(), True),  # Ký hiệu (VD: USDT, SHIB) - Lấy từ bảng Dim Token
    StructField("token_type", StringType(), True),  # 'ERC20' hoặc 'ERC721'

    # Thông tin chuyển nhận
    StructField("from_address", StringType(), True),  # Người chuyển token
    StructField("to_address", StringType(), True),  # Người nhận token

    # Giá trị
    StructField("amount_raw", DecimalType(38, 0), True),  # Giá trị gốc trên Blockchain (VD: 1000000)
    StructField("token_decimals", IntegerType(), True),  # Số thập phân (VD: 6)
    StructField("amount_display", DecimalType(38, 18), True)  # Giá trị thực tế (VD: 1.0) = Raw / 10^Decimals
])


# ==============================================================================
# HÀM TIỆN ÍCH: XUẤT RA CSV RỖNG (HEADER ONLY)
# Giúp bạn tạo template hoặc kiểm tra schema
# ==============================================================================
def generate_csv_template(spark_session, output_dir="data/templates"):
    """
    Tạo các file CSV rỗng chỉ chứa header dựa trên schema đã định nghĩa.
    """
    import os

    # Tạo dữ liệu rỗng
    df_native_empty = spark_session.createDataFrame([], native_tx_schema)
    df_token_empty = spark_session.createDataFrame([], token_transfer_schema)

    # Ghi ra CSV
    print(f"Generatin CSV templates to {output_dir}...")

    df_native_empty.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"{output_dir}/fact_native_transactions_template")

    df_token_empty.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"{output_dir}/fact_token_transfers_template")

    print("✅ Done. Check your data folder.")


# Đoạn code dưới đây chỉ chạy khi bạn execute file này trực tiếp
if __name__ == "__main__":
    from pyspark.sql import SparkSession

    # Khởi tạo Spark Session cục bộ để test
    spark = SparkSession.builder \
        .appName("Schema Definition Test") \
        .master("local[1]") \
        .getOrCreate()

    generate_csv_template(spark)
    spark.stop()
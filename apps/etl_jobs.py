from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType
from pyspark.sql.functions import col, from_unixtime

# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
   .appName("Ethereum Join Analysis") \
   .getOrCreate()

# 2. Định nghĩa Schema (Rất quan trọng để Spark chạy nhanh và chính xác)
# Schema cho transactions.csv (chỉ lấy các cột cần thiết)
tx_schema = StructType()

# Schema cho token_transfers.csv
transfer_schema = StructType()

# 3. Đọc dữ liệu từ thư mục được mount (Lưu ý đường dẫn /opt/spark/data)
print(">>> Đang đọc dữ liệu...")
df_tx = spark.read.csv("/opt/spark/data/transactions*.csv", header=True, schema=tx_schema)
df_transfers = spark.read.csv("/opt/spark/data/token_transfers*.csv", header=True, schema=transfer_schema)

# 4. Xử lý và Đổi tên cột để tránh trùng lặp khi Join
# Ta cần phân biệt: Người gửi ETH (tx_from) khác với Người gửi Token (token_from)
df_tx_clean = df_tx.select(
    col("hash").alias("tx_hash"),
    col("from_address").alias("initiator_address"), # Người khởi tạo giao dịch (trả gas)
    col("gas_price"),
    col("block_timestamp")
)

df_transfers_clean = df_transfers.select(
    col("transaction_hash"),
    col("token_address"),
    col("from_address").alias("token_sender"),   # Người chuyển token
    col("to_address").alias("token_receiver"),   # Người nhận token
    col("value").alias("token_value")            # Số lượng token
)

# 5. Thực hiện JOIN (Left Join)
# Logic: Lấy bảng Transfer làm gốc, nối thêm thông tin thời gian và người trả phí từ bảng Transaction
print(">>> Đang thực hiện Join...")
df_joined = df_transfers_clean.join(
    df_tx_clean,
    df_transfers_clean.transaction_hash == df_tx_clean.tx_hash,
    "left"
)

# 6. Làm sạch và Format lại dữ liệu cuối cùng
final_df = df_joined.select(
    col("transaction_hash"),
    from_unixtime(col("block_timestamp")).alias("datetime"), # Chuyển timestamp sang ngày giờ dễ đọc
    col("initiator_address"),
    col("token_address"),
    col("token_sender"),
    col("token_receiver"),
    col("token_value"),
    col("gas_price")
)

# 7. Ghi ra file CSV kết quả (Gộp vào 1 file duy nhất để dễ xem)
print(">>> Đang ghi file kết quả...")
output_path = "/opt/spark/data/analysed_token_transfers.csv"

# coalesce(1) ép toàn bộ dữ liệu về 1 file duy nhất (chỉ dùng khi dữ liệu < 1GB)
final_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

print(f">>> Hoàn tất! Kiểm tra file tại thư mục data/analysed_token_transfers.csv")
spark.stop()
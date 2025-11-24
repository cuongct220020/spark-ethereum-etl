from pyspark.sql import SparkSession
import os

# Tạo Spark Session
spark = SparkSession.builder \
    .appName("Ethereum ETL Job") \
    .getOrCreate()

print("✅ Spark Session created successfully. Running Job...")

# Lấy đường dẫn tuyệt đối
base_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Đọc CSV file (sử dụng đường dẫn tuyệt đối)
csv_file = os.path.join(project_root, "data", "blocks_20659158_20659258.csv")

print(f"📂 Đang đọc file: {csv_file}")

try:
    df = spark.read.csv(csv_file, header=True, inferSchema=True)

    print(f"✅ Đọc file thành công!")
    print(f"📊 Số lượng blocks: {df.count()}")
    print(f"📋 Schema:")
    df.printSchema()

    print("🔍 Sample data:")
    df.show(5)

    # Thực hiện transformations
    print("\n📈 Thống kê cơ bản:")
    df.describe().show()

    # Lưu kết quả
    output_path = os.path.join(project_root, "data", "processed_blocks")
    print(f"\n💾 Lưu kết quả vào: {output_path}")

    df.write.mode("overwrite").parquet(output_path)

    print("✅ Job hoàn thành thành công!")

except Exception as e:
    print(f"❌ Lỗi: {e}")
    raise

finally:
    spark.stop()
    print("🛑 Spark Session đã dừng.")
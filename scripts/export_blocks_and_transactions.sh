#!/bin/bash

# --- 1. Cấu hình đường dẫn thông minh ---
# Lấy đường dẫn thư mục chứa script này (tức là thư mục /scripts)
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
# Lấy đường dẫn thư mục gốc dự án (lùi lại 1 cấp)
PROJECT_ROOT="$SCRIPT_DIR/.."
# Đường dẫn folder data
DATA_DIR="$PROJECT_ROOT/data"

# --- 2. Load biến môi trường từ file .env ở root ---
if [ -f "$PROJECT_ROOT/.env" ]; then
    # Dùng 'set -a' để tự động export các biến trong .env
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
else
    echo "❌ Lỗi: Không tìm thấy file .env tại $PROJECT_ROOT"
    exit 1
fi

# --- 3. Xử lý tham số đầu vào (Arguments) ---
# $1: Start Block (Bắt buộc)
# $2: End Block (Bắt buộc)
# $3: Batch Size (Tùy chọn - mặc định lấy từ .env hoặc là 1)
# $4: Max Workers (Tùy chọn - mặc định lấy từ .env hoặc là 1)

START_BLOCK=$1
END_BLOCK=$2
BATCH_SIZE=${3:-${DEFAULT_BATCH_SIZE:-1}}
MAX_WORKERS=${4:-${DEFAULT_MAX_WORKERS:-1}}

# Kiểm tra nếu thiếu Start hoặc End block
if [ -z "$START_BLOCK" ] || [ -z "$END_BLOCK" ]; then
    echo "⚠️  Cách dùng: ./scripts/export_blocks_and_transactions.sh <START> <END> [BATCH_SIZE] [WORKERS]"
    echo "👉 Ví dụ: ./scripts/export_blocks_and_transactions.sh 18000000 18000100 10 5"
    exit 1
fi

# --- 4. Định nghĩa tên file Output ---
# File sẽ được lưu vào folder /data với tên có chứa start và end block
BLOCKS_FILE="$DATA_DIR/blocks_${START_BLOCK}_${END_BLOCK}.csv"
TXS_FILE="$DATA_DIR/transactions_${START_BLOCK}_${END_BLOCK}.csv"

echo "=================================================="
echo "🚀 Đang bắt đầu Export dữ liệu Ethereum..."
echo "📦 Block range: $START_BLOCK -> $END_BLOCK"
echo "⚙️  Cấu hình: Batch Size = $BATCH_SIZE | Workers = $MAX_WORKERS"
echo "📂 Output Folder: $DATA_DIR"
echo "🔗 Provider: $PROVIDER_URI"
echo "=================================================="

# --- 5. Chạy lệnh Ethereum ETL ---
ethereumetl export_blocks_and_transactions \
    --start-block "$START_BLOCK" \
    --end-block "$END_BLOCK" \
    --provider-uri "$PROVIDER_URI" \
    --batch-size "$BATCH_SIZE" \
    --max-workers "$MAX_WORKERS" \
    --blocks-output "$BLOCKS_FILE" \
    --transactions-output "$TXS_FILE"

# Kiểm tra kết quả
if [ $? -eq 0 ]; then
    echo "✅ Thành công! Kiểm tra file tại folder 'data'."
else
    echo "❌ Có lỗi xảy ra trong quá trình chạy lệnh."
fi
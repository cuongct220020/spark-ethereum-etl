#!/bin/bash

# --- 1. Cấu hình đường dẫn ---
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/.."
DATA_DIR="$PROJECT_ROOT/data"

# --- 2. Load .env ---
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
else
    echo "❌ Lỗi: Không tìm thấy file .env"
    exit 1
fi

# --- 3. Xử lý tham số ---
START_BLOCK=$1
END_BLOCK=$2
BATCH_SIZE=${3:-${DEFAULT_BATCH_SIZE:-1}} # Mặc định là 1
MAX_WORKERS=${4:-${DEFAULT_MAX_WORKERS:-1}} # Mặc định là 1

if [ -z "$START_BLOCK" ] || [ -z "$END_BLOCK" ]; then
    echo "⚠️  Cách dùng: ./scripts/export_receipts_and_logs.sh <START> <END> [BATCH] [WORKERS]"
    exit 1
fi

# Định nghĩa tên các file
TXS_INPUT_FILE="$DATA_DIR/transactions_${START_BLOCK}_${END_BLOCK}.csv"
HASHES_FILE="$DATA_DIR/transaction_hashes_${START_BLOCK}_${END_BLOCK}.txt"
RECEIPTS_FILE="$DATA_DIR/receipts_${START_BLOCK}_${END_BLOCK}.csv"
LOGS_FILE="$DATA_DIR/logs_${START_BLOCK}_${END_BLOCK}.csv"

# Kiểm tra file input có tồn tại không
if [ ! -f "$TXS_INPUT_FILE" ]; then
    echo "❌ Lỗi: Không tìm thấy file giao dịch đầu vào: $TXS_INPUT_FILE"
    echo "👉 Bạn cần chạy lệnh export_blocks_and_transactions trước."
    exit 1
fi

echo "=================================================="

# --- TỐI ƯU HÓA: Chỉ trích xuất nếu file chưa tồn tại ---
if [ -f "$HASHES_FILE" ]; then
    echo "✅ File hashes đã tồn tại ($HASHES_FILE)."
    echo "⏩ Bỏ qua bước trích xuất để tiết kiệm thời gian."
else
    echo "🧾 Bước 1: Trích xuất Transaction Hashes..."
    ethereumetl extract_csv_column \
        --input "$TXS_INPUT_FILE" \
        --column hash \
        --output "$HASHES_FILE"
fi

echo "📡 Bước 2: Tải Receipts và Logs (Provider: Alchemy/Infura)..."
echo "⚙️  Batch Size: $BATCH_SIZE | Workers: $MAX_WORKERS"

# Chạy lệnh export
ethereumetl export_receipts_and_logs \
    --transaction-hashes "$HASHES_FILE" \
    --provider-uri "$PROVIDER_URI" \
    --receipts-output "$RECEIPTS_FILE" \
    --logs-output "$LOGS_FILE" \
    --batch-size "$BATCH_SIZE" \
    --max-workers "$MAX_WORKERS"

# Kiểm tra kết quả
if [ $? -eq 0 ]; then
    echo "✅ Xong! Đã xóa file hash tạm để giải phóng ổ cứng."
    rm "$HASHES_FILE" # Chỉ xóa khi thành công hoàn toàn
    echo "📂 Output: $LOGS_FILE và $RECEIPTS_FILE"
else
    echo "❌ Có lỗi xảy ra khi tải dữ liệu."
    echo "💡 File hash tạm vẫn được giữ lại để bạn chạy lại lệnh lần sau."
fi
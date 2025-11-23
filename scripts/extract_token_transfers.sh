#!/bin/bash

# --- 1. Cấu hình đường dẫn ---
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/.."
DATA_DIR="$PROJECT_ROOT/data"

# --- 2. Xử lý tham số ---
START_BLOCK=$1
END_BLOCK=$2
BATCH_SIZE=${3:-100} # Xử lý local nên để batch to cho nhanh
MAX_WORKERS=${4:-1}

if [ -z "$START_BLOCK" ] || [ -z "$END_BLOCK" ]; then
    echo "⚠️  Cách dùng: ./scripts/extract_token_transfers.sh <START> <END>"
    exit 1
fi

# Định nghĩa tên file
LOGS_INPUT_FILE="$DATA_DIR/logs_${START_BLOCK}_${END_BLOCK}.csv"
TOKEN_TRANSFERS_OUTPUT="$DATA_DIR/token_transfers_${START_BLOCK}_${END_BLOCK}.csv"

# Kiểm tra đầu vào
if [ ! -f "$LOGS_INPUT_FILE" ]; then
    echo "❌ Lỗi: Không tìm thấy file Logs: $LOGS_INPUT_FILE"
    echo "👉 Bạn cần chạy lệnh export_receipts_and_logs trước."
    exit 1
fi

echo "=================================================="
echo "🪙  Đang giải mã Token Transfers từ Logs..."
echo "📂 Input: $LOGS_INPUT_FILE"
echo "=================================================="

ethereumetl extract_token_transfers \
    --logs "$LOGS_INPUT_FILE" \
    --output "$TOKEN_TRANSFERS_OUTPUT" \
    --batch-size "$BATCH_SIZE" \
    --max-workers "$MAX_WORKERS"

if [ $? -eq 0 ]; then
    echo "✅ Thành công! File kết quả:"
    echo "👉 $TOKEN_TRANSFERS_OUTPUT"
else
    echo "❌ Có lỗi xảy ra."
fi
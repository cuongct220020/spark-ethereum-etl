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
MAX_WORKERS=${3:-5}

if [ -z "$START_BLOCK" ] || [ -z "$END_BLOCK" ]; then
    echo "⚠️  Cách dùng: ./scripts/export_tokens.sh <START> <END> [WORKERS]"
    exit 1
fi

# --- ĐỊNH NGHĨA FILE ---
# INPUT: Vẫn lấy từ token_transfers.csv (Nguồn chuẩn nhất)
INPUT_FILE="$DATA_DIR/token_transfers_${START_BLOCK}_${END_BLOCK}.csv"
# TEMP: File danh sách địa chỉ
TOKEN_ADDR_FILE="$DATA_DIR/token_addresses_${START_BLOCK}_${END_BLOCK}.txt"
# OUTPUT: File kết quả dạng CSV
TOKENS_OUTPUT="$DATA_DIR/tokens_${START_BLOCK}_${END_BLOCK}.csv"

# Kiểm tra file input
if [ ! -f "$INPUT_FILE" ]; then
    echo "❌ Lỗi: Không tìm thấy file input: $INPUT_FILE"
    exit 1
fi

echo "=================================================="
echo "🔍 Bước 1: Lấy danh sách Token Address..."
echo "📂 Nguồn: $INPUT_FILE"

# Lấy danh sách địa chỉ duy nhất từ cột 1
cut -d ',' -f 1 "$INPUT_FILE" | sed '1d' | sort | uniq > "$TOKEN_ADDR_FILE"

COUNT=$(wc -l < "$TOKEN_ADDR_FILE" | xargs)
echo "📊 Tìm thấy $COUNT loại token khác nhau."

if [ "$COUNT" -eq "0" ]; then
    echo "⚠️  Không tìm thấy token nào."
    exit 0
fi

echo "📡 Bước 2: Tải thông tin Token (Symbol, Decimals)..."
echo "⚙️  Output: $TOKENS_OUTPUT"

ethereumetl export_tokens \
    --token-addresses "$TOKEN_ADDR_FILE" \
    --provider-uri "$PROVIDER_URI" \
    --output "$TOKENS_OUTPUT" \
    --max-workers "$MAX_WORKERS"

if [ $? -eq 0 ]; then
    echo "✅ Xong! File Tokens (CSV) lưu tại:"
    echo "👉 $TOKENS_OUTPUT"
    # Xóa file tạm
    rm "$TOKEN_ADDR_FILE"
else
    echo "❌ Có lỗi xảy ra khi gọi API."
fi
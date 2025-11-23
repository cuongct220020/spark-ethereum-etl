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
BATCH_SIZE=${3:-10}
MAX_WORKERS=${4:-5}

if [ -z "$START_BLOCK" ] || [ -z "$END_BLOCK" ]; then
    echo "⚠️  Cách dùng: ./scripts/export_contracts.sh <START> <END> [BATCH] [WORKERS]"
    exit 1
fi

# --- ĐỊNH NGHĨA FILE ---
# INPUT: Lấy từ file token_transfers (File này bạn đã có và chắc chắn có dữ liệu)
INPUT_FILE="$DATA_DIR/token_transfers_${START_BLOCK}_${END_BLOCK}.csv"
# TEMP: File chứa danh sách địa chỉ để quét
CONTRACT_ADDR_FILE="$DATA_DIR/contract_addresses_${START_BLOCK}_${END_BLOCK}.txt"
# OUTPUT: File kết quả dạng CSV
CONTRACTS_OUTPUT="$DATA_DIR/contracts_${START_BLOCK}_${END_BLOCK}.csv"

# Kiểm tra file input
if [ ! -f "$INPUT_FILE" ]; then
    echo "❌ Lỗi: Không tìm thấy file Token Transfers: $INPUT_FILE"
    echo "👉 Hãy chạy script 'extract_token_transfers.sh' trước."
    exit 1
fi

echo "=================================================="
echo "🏗️  Bước 1: Trích xuất địa chỉ Contract từ Token Transfers..."
echo "📂 Nguồn: $INPUT_FILE"

# Lệnh này lấy cột 1 (token_address), bỏ dòng header, sort và uniq để lấy danh sách duy nhất
cut -d ',' -f 1 "$INPUT_FILE" | sed '1d' | sort | uniq > "$CONTRACT_ADDR_FILE"

COUNT=$(wc -l < "$CONTRACT_ADDR_FILE" | xargs)
echo "📊 Tìm thấy $COUNT địa chỉ contract cần lấy thông tin."

if [ "$COUNT" -eq "0" ]; then
    echo "⚠️  Không tìm thấy địa chỉ nào."
    exit 0
fi

echo "📡 Bước 2: Tải thông tin Contracts (Bytecode, ERC type)..."
echo "⚙️  Provider: $PROVIDER_URI"
echo "⚙️  Output: $CONTRACTS_OUTPUT"

ethereumetl export_contracts \
    --contract-addresses "$CONTRACT_ADDR_FILE" \
    --provider-uri "$PROVIDER_URI" \
    --output "$CONTRACTS_OUTPUT" \
    --batch-size "$BATCH_SIZE" \
    --max-workers "$MAX_WORKERS"

if [ $? -eq 0 ]; then
    echo "✅ Thành công! File contracts (CSV) lưu tại:"
    echo "👉 $CONTRACTS_OUTPUT"
    # Xóa file tạm
    rm "$CONTRACT_ADDR_FILE"
else
    echo "❌ Có lỗi xảy ra."
fi
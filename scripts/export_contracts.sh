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
BATCH_SIZE=${3:-${DEFAULT_BATCH_SIZE:-100}}
MAX_WORKERS=${4:-${DEFAULT_MAX_WORKERS:-5}}

if [ -z "$START_BLOCK" ] || [ -z "$END_BLOCK" ]; then
    echo "⚠️  Cách dùng: ./scripts/export_contracts.sh <START> <END> [BATCH] [WORKERS]"
    exit 1
fi

# Định nghĩa tên các file
RECEIPTS_INPUT_FILE="$DATA_DIR/receipts_${START_BLOCK}_${END_BLOCK}.csv"
ADDRESSES_FILE="$DATA_DIR/contract_addresses_${START_BLOCK}_${END_BLOCK}.txt"
CONTRACTS_FILE="$DATA_DIR/contracts_${START_BLOCK}_${END_BLOCK}.csv"

# Kiểm tra file input có tồn tại không
if [ ! -f "$RECEIPTS_INPUT_FILE" ]; then
    echo "❌ Lỗi: Không tìm thấy file receipts đầu vào: $RECEIPTS_INPUT_FILE"
    echo "👉 Bạn cần chạy lệnh export_receipts_and_logs trước."
    exit 1
fi

echo "=================================================="
echo "🏗️  EXPORT CONTRACTS"
echo "=================================================="

# --- TỐI ƯU HÓA: Chỉ trích xuất nếu file chưa tồn tại ---
if [ -f "$ADDRESSES_FILE" ]; then
    echo "✅ File contract addresses đã tồn tại ($ADDRESSES_FILE)."
    echo "⏩ Bỏ qua bước trích xuất để tiết kiệm thời gian."
else
    echo "🔍 Bước 1: Trích xuất Contract Addresses từ Receipts..."
    ethereumetl extract_csv_column \
        --input "$RECEIPTS_INPUT_FILE" \
        --column contract_address \
        --output "$ADDRESSES_FILE"

    # Kiểm tra nếu file rỗng hoặc không có địa chỉ hợp lệ
    if [ ! -s "$ADDRESSES_FILE" ]; then
        echo "❌ Lỗi: Không tìm thấy địa chỉ contract nào trong file receipts."
        rm -f "$ADDRESSES_FILE"
        exit 1
    fi
fi

echo "📡 Bước 2: Tải Contract Data (bytecode, function sighash)..."
echo "⚙️  Batch Size: $BATCH_SIZE | Workers: $MAX_WORKERS"
echo "🔗 Provider: $PROVIDER_URI"

# Chạy lệnh export (ẩn warning không quan trọng)
ethereumetl export_contracts \
    --contract-addresses "$ADDRESSES_FILE" \
    --provider-uri "$PROVIDER_URI" \
    --batch-size "$BATCH_SIZE" \
    --max-workers "$MAX_WORKERS" \
    --output "$CONTRACTS_FILE" 2>&1 | grep -v "pkg_resources\|evmdasm.disassembler"

# Kiểm tra kết quả
if [ $? -eq 0 ]; then
    echo "✅ Xong! Đã xóa file addresses tạm để giải phóng ổ cứng."
    rm "$ADDRESSES_FILE" # Chỉ xóa khi thành công hoàn toàn
    echo "📂 Output: $CONTRACTS_FILE"
    echo "=================================================="
else
    echo "❌ Có lỗi xảy ra khi tải dữ liệu contracts."
    echo "💡 File addresses tạm vẫn được giữ lại để bạn chạy lại lệnh lần sau."
    exit 1
fi
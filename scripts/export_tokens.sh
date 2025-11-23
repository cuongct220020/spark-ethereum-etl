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
MAX_WORKERS=${3:-${DEFAULT_MAX_WORKERS:-5}}

if [ -z "$START_BLOCK" ] || [ -z "$END_BLOCK" ]; then
    echo "⚠️  Cách dùng: ./scripts/export_tokens.sh <START> <END> [WORKERS]"
    exit 1
fi

# Định nghĩa tên các file
TOKEN_TRANSFERS_FILE="$DATA_DIR/token_transfers_${START_BLOCK}_${END_BLOCK}.csv"
CONTRACTS_INPUT_FILE="$DATA_DIR/contracts_${START_BLOCK}_${END_BLOCK}.csv"
TOKEN_ADDRESSES_FILE="$DATA_DIR/token_addresses_${START_BLOCK}_${END_BLOCK}.txt"
TOKENS_FILE="$DATA_DIR/tokens_${START_BLOCK}_${END_BLOCK}.csv"

echo "=================================================="
echo "🪙  EXPORT TOKENS (ERC20 & ERC721)"
echo "=================================================="

# --- TỐI ƯU HÓA: Chỉ trích xuất nếu file chưa tồn tại ---
if [ -f "$TOKEN_ADDRESSES_FILE" ]; then
    echo "✅ File token addresses đã tồn tại ($TOKEN_ADDRESSES_FILE)."
    echo "⏩ Bỏ qua bước trích xuất để tiết kiệm thời gian."
else
    echo "🔍 Bước 1: Trích xuất Token Addresses từ nhiều nguồn..."

    # Tạo file tạm để merge addresses từ nhiều nguồn
    TEMP_ADDRESSES="$DATA_DIR/temp_token_addresses_${START_BLOCK}_${END_BLOCK}.txt"
    > "$TEMP_ADDRESSES"  # Tạo file rỗng

    # Nguồn 1: Lấy từ token_transfers (ưu tiên vì có nhiều data hơn)
    if [ -f "$TOKEN_TRANSFERS_FILE" ]; then
        echo "  📋 Đang lấy token addresses từ token_transfers..."
        ethereumetl extract_csv_column \
            --input "$TOKEN_TRANSFERS_FILE" \
            --column token_address \
            --output - >> "$TEMP_ADDRESSES" 2>/dev/null || true
    fi

    # Nguồn 2: Lấy từ contracts (nếu có)
    if [ -f "$CONTRACTS_INPUT_FILE" ]; then
        echo "  📋 Đang lấy token addresses từ contracts..."
        ethereumetl extract_csv_column \
            --input "$CONTRACTS_INPUT_FILE" \
            --column address \
            --output - | \
        while read addr; do
            # Chỉ lấy những contract có is_erc20 hoặc is_erc721
            if grep -q "$addr" "$CONTRACTS_INPUT_FILE"; then
                line=$(grep "$addr" "$CONTRACTS_INPUT_FILE")
                if echo "$line" | grep -qE "True.*True|True.*False|False.*True"; then
                    echo "$addr" >> "$TEMP_ADDRESSES"
                fi
            fi
        done 2>/dev/null || true
    fi

    # Loại bỏ duplicate và sort
    sort -u "$TEMP_ADDRESSES" > "$TOKEN_ADDRESSES_FILE"
    rm -f "$TEMP_ADDRESSES"

    # Kiểm tra nếu file rỗng
    if [ ! -s "$TOKEN_ADDRESSES_FILE" ]; then
        echo "⚠️  Cảnh báo: Không tìm thấy token address nào."
        echo "📌 Điều này xảy ra khi:"
        echo "    - Không có token_transfers_${START_BLOCK}_${END_BLOCK}.csv"
        echo "    - Không có contracts_${START_BLOCK}_${END_BLOCK}.csv với is_erc20/is_erc721"
        rm -f "$TOKEN_ADDRESSES_FILE"
        exit 0
    fi

    # Đếm số lượng token tìm thấy
    TOKEN_COUNT=$(wc -l < "$TOKEN_ADDRESSES_FILE" | tr -d ' ')
    echo "✅ Tìm thấy $TOKEN_COUNT unique token addresses"
fi

echo "📡 Bước 2: Tải Token Metadata (name, symbol, decimals, total_supply)..."
echo "⚙️  Workers: $MAX_WORKERS"
echo "🔗 Provider: $PROVIDER_URI"

# Chạy lệnh export
ethereumetl export_tokens \
    --token-addresses "$TOKEN_ADDRESSES_FILE" \
    --provider-uri "$PROVIDER_URI" \
    --max-workers "$MAX_WORKERS" \
    --output "$TOKENS_FILE"

# Kiểm tra kết quả
if [ $? -eq 0 ]; then
    echo "✅ Xong! Đã xóa file token addresses tạm để giải phóng ổ cứng."
    rm "$TOKEN_ADDRESSES_FILE" # Chỉ xóa khi thành công hoàn toàn
    echo "📂 Output: $TOKENS_FILE"
    echo "=================================================="
else
    echo "❌ Có lỗi xảy ra khi tải dữ liệu tokens."
    echo "💡 File token addresses tạm vẫn được giữ lại để bạn chạy lại lệnh lần sau."
    exit 1
fi
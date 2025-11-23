#!/bin/bash

# --- 1. Cấu hình đường dẫn ---
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$SCRIPT_DIR/.."

# --- 2. Load biến môi trường từ file .env ---
if [ -f "$PROJECT_ROOT/.env" ]; then
    set -a
    source "$PROJECT_ROOT/.env"
    set +a
else
    echo "❌ Lỗi: Không tìm thấy file .env tại $PROJECT_ROOT"
    exit 1
fi

# --- 3. Xử lý tham số ngày tháng ---
# $1: Date (Yêu cầu định dạng YYYY-MM-DD)
DATE=$1

if [ -z "$DATE" ]; then
    echo "⚠️  Cách dùng: ./scripts/get_block_range.sh <YYYY-MM-DD>"
    echo "👉 Ví dụ: ./scripts/get_block_range.sh 2023-01-01"
    exit 1
fi

echo "=================================================="
echo "📅 Đang tìm Block Range cho ngày: $DATE"
echo "🔗 Provider: $PROVIDER_URI"
echo "=================================================="

# --- 4. Chạy lệnh ---
# Lưu kết quả vào biến RESULT để hiển thị cho đẹp
RESULT=$(ethereumetl get_block_range_for_date \
    --provider-uri "$PROVIDER_URI" \
    --date "$DATE")

if [ $? -eq 0 ]; then
    echo "✅ Kết quả: $RESULT"
    echo "--------------------------------------------------"
    echo "💡 Gợi ý: Copy dòng trên để chạy lệnh export:"
    # Tự động tách chuỗi để gợi ý lệnh tiếp theo (Optional)
    IFS=',' read -r START_BLOCK END_BLOCK <<< "$RESULT"
    echo "./scripts/export_blocks_and_transactions.sh $START_BLOCK $END_BLOCK"
else
    echo "❌ Lỗi: Không thể lấy dữ liệu. Kiểm tra lại ngày hoặc API Key."
fi
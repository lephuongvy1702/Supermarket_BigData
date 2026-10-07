#!/bin/bash
# ============================================================================
# RUN FLUME AGENT SCRIPT
# Khởi chạy Flume agent để thu thập dữ liệu từ thư mục log và ghi vào HDFS
# ============================================================================

echo "=========================================="
echo "🚀 FLUME AGENT - SUPERMARKET LOG COLLECTOR"
echo "=========================================="

# Cấu hình đường dẫn
FLUME_HOME=/opt/flume
CONF_DIR=$(dirname "$0")/flume
AGENT_NAME=agent
CONF_FILE=$CONF_DIR/flume_agent.conf

# Kiểm tra Flume đã được cài đặt
if [ ! -d "$FLUME_HOME" ]; then
    echo "❌ Lỗi: Flume không được tìm thấy tại $FLUME_HOME"
    echo "Vui lòng cài đặt Flume hoặc cập nhật FLUME_HOME"
    exit 1
fi

# Kiểm tra file cấu hình
if [ ! -f "$CONF_FILE" ]; then
    echo "❌ Lỗi: File cấu hình không tồn tại: $CONF_FILE"
    exit 1
fi

# Kiểm tra HDFS đã chạy
echo "🔍 Kiểm tra HDFS..."
if ! hdfs dfs -test -d /; then
    echo "⚠️  Cảnh báo: Không thể kết nối đến HDFS"
    echo "Hãy đảm bảo Hadoop đang chạy với lệnh: start-dfs.sh"
    read -p "Bạn có muốn tiếp tục? (y/n): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Tạo thư mục trên HDFS nếu chưa tồn tại
echo "📁 Tạo thư mục trên HDFS..."
hdfs dfs -mkdir -p /data/raw/supermarket/logs
hdfs dfs -chmod 777 /data/raw/supermarket/logs

# Hiển thị thông tin
echo ""
echo "=========================================="
echo "📋 THÔNG TIN CẤU HÌNH"
echo "=========================================="
echo "📂 FLUME_HOME: $FLUME_HOME"
echo "📄 CONF_FILE: $CONF_FILE"
echo "🆔 AGENT_NAME: $AGENT_NAME"
echo "📁 LOG_DIR: /home/your_username/supermarket_logs"
echo "🗄️  HDFS_PATH: /data/raw/supermarket/logs"
echo "=========================================="
echo ""

# Khởi chạy Flume
echo "🚀 Khởi chạy Flume agent..."
echo "ℹ️  Nhấn Ctrl+C để dừng"
echo ""

$FLUME_HOME/bin/flume-ng agent \
    --name $AGENT_NAME \
    --conf-file $CONF_FILE \
    --conf $FLUME_HOME/conf \
    -Dflume.root.logger=INFO,console

echo ""
echo "=========================================="
echo "🛑 Flume agent đã dừng"
echo "=========================================="
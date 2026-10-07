#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SUPERMARKET LOG GENERATOR - BIG DATA PIPELINE
Công đoạn: Thu thập dữ liệu theo luồng (Streaming Acquisition)
Mô tả: Sinh log siêu thị liên tục, ghi vào file để Flume thu thập và đẩy lên HDFS
"""

import time
import random
import json
import os
import signal
import sys
from datetime import datetime
from pathlib import Path
import mysql.connector

# ============ CẤU HÌNH ============

# Cấu hình đường dẫn - THAY ĐỔI THEO MÁY CỦA BẠN
LOG_DIR = "/home/vy/supermarket_logs"
LOG_FILE = os.path.join(LOG_DIR, "supermarket_logs.json")

# Dữ liệu mô phỏng
CUSTOMERS = [f"KH_{i:03d}" for i in range(1, 41)]

PRODUCTS = {
    "Sữa tươi": 35000,
    "Bánh mì": 15000,
    "Trứng gà": 42000,
    "Gạo ST25": 180000,
    "Thịt heo": 120000,
    "Rau cải": 12000,
    "Táo Mỹ": 80000,
    "Bia Heineken": 32000,
    "Nước ngọt": 10000,
    "Kem đánh răng": 45000,
    "Mì tôm": 8000,
    "Dầu ăn": 55000,
    "Đường trắng": 25000,
    "Muối iốt": 10000,
    "Sữa chua": 12000
}

VALID_ACTIONS = {
    "VAO_CUA": "ENTER",
    "THEM_GIO": "ADD_TO_CART",
    "THANH_TOAN": "CHECKOUT",
    "RA_VE": "EXIT"
}

# Biến toàn cục
customer_state = {}
shift_id = 1
shift_start = datetime.now()
shift_stats = {"total_customers": 0, "total_revenue": 0, "products_sold": {}}
logs = []
running = True

HISTORY_FILE = os.path.join(LOG_DIR, "shift_history.json")

# ============ KHỞI TẠO ============

def init_customer_state():
    """Khởi tạo trạng thái cho tất cả khách hàng"""
    return {customer: {"status": "outside", "cart": [], "total": 0} for customer in CUSTOMERS}

def init_log_directory():
    """Tạo thư mục log nếu chưa tồn tại"""
    Path(LOG_DIR).mkdir(parents=True, exist_ok=True)
    print(f"Thư mục log: {LOG_DIR}")

def signal_handler(sig, frame):
    """Xử lý tín hiệu dừng chương trình"""
    global running
    print("\n\nĐang dừng chương trình...")
    running = False

# ============ CÁC HÀM XỬ LÝ LOG ============

def get_next_action(state):
    """Xác định hành động tiếp theo của khách hàng dựa trên trạng thái hiện tại"""
    if state["status"] == "outside":
        return "VAO_CUA"
    elif state["status"] == "inside":
        if state["total"] == 0:
            # Chưa mua gì: 80% thêm vào giỏ, 20% ra về
            return "THEM_GIO" if random.random() < 0.8 else "RA_VE"
        else:
            # Đã có hàng trong giỏ
            r = random.random()
            if r < 0.4:
                return "THEM_GIO"      # 40% tiếp tục mua
            elif r < 0.8:
                return "THANH_TOAN"    # 40% thanh toán
            else:
                return "RA_VE"         # 20% ra về
    elif state["status"] == "checkout":
        return "RA_VE"
    return "VAO_CUA"

def write_to_mysql(log_entry):
    """Ghi dữ liệu trực tiếp vào bảng orders trong MySQL"""
    try:
        # Cấu hình kết nối - Vy kiểm tra lại password của máy Vy nhé
        conn = mysql.connector.connect(
            host="localhost",
            user="vy",
            password="123", 
            database="supermarket_db"
        )
        cursor = conn.cursor()
        
        # Câu lệnh SQL INSERT - Khớp với các trường trong log_entry
        sql = """INSERT INTO orders (customer_id, action_code, product_name, quantity, amount, timestamp) 
                 VALUES (%s, %s, %s, %s, %s, %s)"""
        
        val = (
            log_entry['customer_id'],
            log_entry['action_code'],
            log_entry['product'],
            log_entry['quantity'],
            log_entry['amount'],
            log_entry['timestamp']
        )
        
        cursor.execute(sql, val)
        conn.commit() # Quan trọng: Phải có dòng này dữ liệu mới lưu vào DB
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"❌ Lỗi ghi vào MySQL: {e}")

def save_shift_report():
    """Lưu báo cáo ca làm việc"""
    global shift_id, shift_start, shift_stats, logs
    
    shift_end = datetime.now()
    duration = shift_end - shift_start
    hours = duration.seconds // 3600
    minutes = (duration.seconds % 3600) // 60
    seconds = duration.seconds % 60
    
    # Chuẩn bị dữ liệu sản phẩm
    product_list = []
    for product, qty in shift_stats["products_sold"].items():
        product_list.append({
            "product": product, 
            "quantity": qty,
            "revenue": qty * PRODUCTS.get(product, 0)
        })
    product_list.sort(key=lambda x: x["quantity"], reverse=True)
    
    report = {
        "shift_id": shift_id,
        "start_time": shift_start.isoformat(),
        "end_time": shift_end.isoformat(),
        "duration_seconds": duration.seconds,
        "duration_str": f"{hours}h{minutes}m{seconds}s",
        "total_customers": shift_stats["total_customers"],
        "total_revenue": shift_stats["total_revenue"],
        "avg_revenue_per_customer": round(shift_stats["total_revenue"] / shift_stats["total_customers"], 2) if shift_stats["total_customers"] > 0 else 0,
        "products_sold": product_list,
        "total_transactions": len(logs),
        "timestamp": datetime.now().isoformat()
    }
    
    # Đọc lịch sử cũ
    history = []
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                history = json.load(f)
        except:
            history = []
    
    history.append(report)
    
    # Ghi vào file
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    
    # In báo cáo
    print("\n" + "=" * 100)
    print(f"BÁO CÁO CA {shift_id}")
    print("=" * 100)
    print(f"MỞ CA: {report['start_time'][:19]}")
    print(f"ĐÓNG CA: {report['end_time'][:19]}")
    print(f"THỜI GIAN: {report['duration_str']}")
    print(f"SỐ KHÁCH: {report['total_customers']}")
    print(f"DOANH THU: {report['total_revenue']:,.0f} VND")
    print(f"TB/KHÁCH: {report['avg_revenue_per_customer']:,.0f} VND")
    print(f"SỐ GIAO DỊCH: {report['total_transactions']}")
    print("-" * 100)
    print("TOP SẢN PHẨM BÁN CHẠY:")
    
    if product_list:
        print(f"   {'STT':<4} | {'Tên sản phẩm':<20} | {'Số lượng':<10} | {'Doanh thu':<15}")
        print(f"   {'-'*4} | {'-'*20} | {'-'*10} | {'-'*15}")
        total_qty = 0
        for idx, item in enumerate(product_list[:10], 1):  # Chỉ hiển thị top 10
            print(f"   {idx:<4} | {item['product']:<20} | {item['quantity']:<10} | {item['revenue']:>13,} VND")
            total_qty += item['quantity']
        if len(product_list) > 10:
            print(f"   ... và {len(product_list) - 10} sản phẩm khác")
    else:
        print("   (Chưa có sản phẩm nào được bán)")
    
    print("=" * 100)

def start_new_shift():
    """Bắt đầu ca làm việc mới"""
    global shift_id, shift_start, shift_stats, logs, customer_state
    
    # Lưu ca cũ nếu có dữ liệu
    if logs:
        save_shift_report()
    
    # Bắt đầu ca mới
    shift_id += 1
    shift_start = datetime.now()
    shift_stats = {"total_customers": 0, "total_revenue": 0, "products_sold": {}}
    logs = []
    customer_state = init_customer_state()
    
    print("\n" + "=" * 100)
    print(f"CHUYỂN CA - BẮT ĐẦU CA {shift_id}")
    print(f"THỜI GIAN: {shift_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 100)

def generate_log():
    """Sinh một log entry mới"""
    global shift_stats, logs, customer_state
    
    customer = random.choice(CUSTOMERS)
    current = customer_state.get(customer, {"status": "outside", "cart": [], "total": 0})
    action = get_next_action(current)
    
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    product_name = ""
    quantity = 0
    amount = 0
    action_display = VALID_ACTIONS[action]
    
    # Xử lý từng hành động
    if action == "VAO_CUA":
        if current["status"] == "outside":
            customer_state[customer] = {"status": "inside", "cart": [], "total": 0}
            shift_stats["total_customers"] += 1
    
    elif action == "THEM_GIO":
        product_name = random.choice(list(PRODUCTS.keys()))
        quantity = random.randint(1, 3)
        unit_price = PRODUCTS[product_name]
        amount = quantity * unit_price
        
        # Cập nhật giỏ hàng
        cart_item = {"product": product_name, "qty": quantity, "price": amount}
        if "cart" not in customer_state[customer]:
            customer_state[customer]["cart"] = []
        customer_state[customer]["cart"].append(cart_item)
        customer_state[customer]["total"] = customer_state[customer].get("total", 0) + amount
        customer_state[customer]["status"] = "inside"
        
        # Cập nhật thống kê
        shift_stats["total_revenue"] += amount
        if product_name in shift_stats["products_sold"]:
            shift_stats["products_sold"][product_name] += quantity
        else:
            shift_stats["products_sold"][product_name] = quantity
    
    elif action == "THANH_TOAN":
        amount = customer_state[customer].get("total", 0)
        if amount > 0:
            customer_state[customer]["status"] = "checkout"
            customer_state[customer]["cart"] = []
        else:
            # Nếu chưa có hàng, tự động chuyển thành ra về
            action = "RA_VE"
            action_display = VALID_ACTIONS["RA_VE"]
            amount = 0
            customer_state[customer]["status"] = "outside"
    
    elif action == "RA_VE":
        customer_state[customer] = {"status": "outside", "cart": [], "total": 0}
    
    # Tạo log entry
    log_entry = {
        "log_type": "supermarket_transaction",
        "source_system": "supermarket_pos",
        "timestamp": timestamp,
        "customer_id": customer,
        "action": action_display,
        "action_code": action,
        "product": product_name,
        "quantity": quantity,
        "amount": amount,
        "amount_vnd": f"{amount:,.0f}",
        "shift_id": shift_id,
        "batch_id": f"{shift_id}_{timestamp.replace(' ', '_').replace(':', '-')}"
    }
    
    logs.append(log_entry)
    
    # Ghi ra file
    write_to_mysql(log_entry)    
    return log_entry

# ============ CHƯƠNG TRÌNH CHÍNH ============

def main():
    """Hàm chính"""
    global running, shift_id, shift_start, customer_state
    
    # Đăng ký xử lý tín hiệu
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Khởi tạo
    init_log_directory()
    customer_state = init_customer_state()
    
    # Xóa file log cũ nếu muốn bắt đầu mới
    if os.path.exists(LOG_FILE):
        print(f"File log cũ tồn tại: {LOG_FILE}")
        response = input("Bạn có muốn xóa file log cũ và bắt đầu mới? (y/n): ")
        if response.lower() == 'y':
            os.remove(LOG_FILE)
            print("Đã xóa file log cũ")
    
    # In thông tin khởi động
    print("\n" + "=" * 100)
    print("SUPERMARKET LOG GENERATOR - BIG DATA PIPELINE")
    print("=" * 100)
    print(f"Thư mục log: {LOG_DIR}")
    print(f"File log: {LOG_FILE}")
    print(f"Ca {shift_id} bắt đầu: {shift_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Số khách hàng mô phỏng: {len(CUSTOMERS)}")
    print(f"Số sản phẩm: {len(PRODUCTS)}")
    print("=" * 100)
    print("HƯỚNG DẪN:")
    print("   - Nhấn Ctrl+C để dừng chương trình")
    print("   - Dữ liệu sẽ tự động ghi vào file mỗi giây")
    print("   - Ca làm việc tự động chuyển sau mỗi 100 giao dịch")
    print("=" * 100)
    print(f"{'Thời gian':<20} | {'Khách':<8} | {'Hành động':<15} | {'Sản phẩm':<15} | {'SL':<4} | {'Thành tiền':<12}")
    print("-" * 100)
    
    try:
        transaction_count = 0
        while running:
            # Sinh log
            log = generate_log()
            
            # Hiển thị log
            amount_str = f"{log['amount']:,.0f}" if log['amount'] > 0 else "0"
            print(f"{log['timestamp']:<20} | {log['customer_id']:<8} | {log['action']:<15} | {log['product']:<15} | {log['quantity']:<4} | {amount_str:>10} VND")
            
            transaction_count += 1
            
            # Tự động chuyển ca sau mỗi 100 giao dịch
            if transaction_count >= 100:
                start_new_shift()
                transaction_count = 0
                print(f"{'Thời gian':<20} | {'Khách':<8} | {'Hành động':<15} | {'Sản phẩm':<15} | {'SL':<4} | {'Thành tiền':<12}")
                print("-" * 100)
            
            # Delay để mô phỏng thời gian thực (0.5-1.5 giây)
            time.sleep(random.uniform(0.5, 1.5))
    
    except KeyboardInterrupt:
        print("\n\nĐã nhận tín hiệu dừng từ người dùng")
    except BrokenPipeError:
        pass
    except Exception as e:
        print(f"\nLỗi: {e}")
    finally:
        # Lưu ca cuối
        if logs:
            save_shift_report()
        
        # Tổng kết
        print("\n" + "=" * 100)
        print("TỔNG KẾT")
        print("=" * 100)
        
        if os.path.exists(HISTORY_FILE):
            try:
                with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                    all_shifts = json.load(f)
                
                total_revenue = sum(s['total_revenue'] for s in all_shifts)
                total_customers = sum(s['total_customers'] for s in all_shifts)
                total_transactions = sum(s.get('total_transactions', 0) for s in all_shifts)
                
                print(f"Tổng số ca: {len(all_shifts)}")
                print(f"Tổng số khách: {total_customers}")
                print(f"Tổng doanh thu: {total_revenue:,.0f} VND")
                print(f"Tổng số giao dịch: {total_transactions}")
                print(f"File lịch sử: {HISTORY_FILE}")
            except Exception as e:
                print(f"Lỗi đọc file lịch sử: {e}")
        
        print("\nKẾT THÚC CHƯƠNG TRÌNH!")
        print("=" * 100)

if __name__ == "__main__":
    main()
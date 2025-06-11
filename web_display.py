# -*- coding: utf-8 -*-
from flask import Flask, render_template
import mysql.connector
from datetime import datetime
import os

app = Flask(__name__)

# Database configuration
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'm2joy',
    'password': 'Liu041121@',
    'database': 'iot_data'
}

def get_db_connection():
    """Create database connection"""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        return conn
    except mysql.connector.Error as err:
        print(f"Database connection error: {err}")
        return None

@app.route('/')
def index():
    """Main page route, display sensor data"""
    conn = get_db_connection()
    if conn is None:
        return "Database connection failed", 500
    
    cursor = None # 初始化cursor
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT client_id, temperature, timestamp 
            FROM sensor_readings 
            ORDER BY timestamp DESC 
            LIMIT 100
        """)
        readings = cursor.fetchall()
        
        # --- 逻辑重构核心部分 ---
        # --- 终极调试循环 ---
        for reading in readings:
            # 1. 格式化时间戳
            reading['timestamp'] = reading['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            
            # 2. 确保温度是浮点数
            temp_value = float(reading['temperature'])
            reading['temperature'] = temp_value
            
            # 3. 在后端直接判断并添加CSS类名，并打印每一步的判断过程
            reading['temp_class'] = '' # 先设置一个默认值

            # print(f"--- 正在处理温度: {temp_value} (类型: {type(temp_value)}) ---")

            if temp_value > 30.0:
                reading['temp_class'] = 'temperature-high'
                # print(f"    判断结果: {temp_value} > 30.0 为 True,  分配 class: 'temperature-high'")
            elif temp_value < 25.0:
                reading['temp_class'] = 'temperature-low'
                # print(f"    判断结果: {temp_value} < 25.0 为 True,  分配 class: 'temperature-low'")
            else:
                # 这个分支处理 25.0 <= temp_value <= 30.0 的情况
                # print(f"    判断结果: 不满足高低温条件, 分配 class: '' (默认)")
                pass

        return render_template('index.html', readings=readings)

    except Exception as e:
        print(f"An error occurred: {e}")
        return "An internal error occurred", 500
    finally:
        if conn and conn.is_connected():
            if cursor:
                cursor.close()
            conn.close()

if __name__ == '__main__':
    if not os.path.exists('templates'):
        os.makedirs('templates')
    
    with open('templates/index.html', 'w', encoding='utf-8') as f:
        f.write('''
<!DOCTYPE html>
<html lang="zh">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>IoT Sensor Data</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { padding: 20px; }
        .table-responsive { margin-top: 20px; }
        .temperature-high { color: #dc3545 !important; font-weight: bold; }
        .temperature-low { color: #0d6efd !important; font-weight: bold; }
    </style>
</head>
<body>
    <div class="container">
        <h1 class="mb-4">IoT Sensor Data</h1>
        <div class="table-responsive">
            <table class="table table-striped table-hover">
                <thead>
                    <tr>
                        <th>Device ID</th>
                        <th>Temperature (°C)</th>
                        <th>Time</th>
                    </tr>
                </thead>
                <tbody>
                    {% for reading in readings %}
                    <tr>
                        <td>{{ reading.client_id }}</td>
                        <td class="{{ reading.temp_class }}">
                            {{ "%.2f"|format(reading.temperature) }}
                        </td>
                        <td>{{ reading.timestamp }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
    </div>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
    <script>
        setTimeout(function() {
            window.location.reload();
        }, 30000);
    </script>
</body>
</html>
        ''')
    
    app.run(host='0.0.0.0', port=5000)
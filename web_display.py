# -*- coding: utf-8 -*-
from flask import Flask, render_template, request, jsonify
import mysql.connector
from datetime import datetime
import os
import paho.mqtt.client as mqtt
import json
import time

app = Flask(__name__)

# Database configuration
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'm2joy',
    'password': 'Liu041121@',
    'database': 'iot_data'
}

# MQTT configuration
MQTT_BROKER_IP = 'localhost'
MQTT_BROKER_PORT = 1883
COMMAND_TOPIC_FORMAT = "stm32/command/{client_id}"

# 创建MQTT客户端
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f"web_control_{int(time.time())}")
mqtt_client.connect(MQTT_BROKER_IP, MQTT_BROKER_PORT)
mqtt_client.loop_start()

def get_db_connection():
    """Create database connection"""
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        return conn
    except mysql.connector.Error as err:
        print(f"Database connection error: {err}")
        return None

def set_device_manual_status(client_id, fan_status=None, light_status=None):
    """
    【核心修复函数】
    当手动控制时，不再更新旧记录，而是插入一条新的状态记录。
    这样可以确保手动操作的状态成为数据库中最新的状态。
    """
    conn = get_db_connection()
    if conn is None: return False
    
    try:
        cursor = conn.cursor(dictionary=True)
        # 1. 获取最新的传感器读数，以便复用
        cursor.execute(
            "SELECT temperature, humidity, light_intensity, fan_status, light_status FROM sensor_readings WHERE client_id = %s ORDER BY timestamp DESC LIMIT 1",
            (client_id,)
        )
        latest_reading = cursor.fetchone()
        if not latest_reading:
            return False

        # 2. 准备新记录的数据
        new_fan_status = fan_status if fan_status is not None else latest_reading['fan_status']
        new_light_status = light_status if light_status is not None else latest_reading['light_status']
        
        # 3. 插入一条新的记录来反映手动操作
        sql = """INSERT INTO sensor_readings 
                 (client_id, temperature, humidity, light_intensity, fan_status, light_status, control_mode) 
                 VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        val = (client_id, latest_reading['temperature'], latest_reading['humidity'], latest_reading['light_intensity'],
               new_fan_status, new_light_status, 'manual')
        
        cursor.execute(sql, val)
        conn.commit()
        return True
    except Exception as e:
        print(f"手动设置设备状态失败: {e}")
        conn.rollback()
        return False
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()

def set_device_auto_mode(client_id):
    """切换回自动模式，同样通过插入新记录实现"""
    conn = get_db_connection()
    if conn is None: return False
    try:
        cursor = conn.cursor(dictionary=True)
        # 1. 获取最新记录的所有数据
        cursor.execute(
            "SELECT * FROM sensor_readings WHERE client_id = %s ORDER BY timestamp DESC LIMIT 1",
            (client_id,)
        )
        latest_reading = cursor.fetchone()
        if not latest_reading: return False

        # 2. 插入一条模式为'auto'的新记录
        sql = """INSERT INTO sensor_readings 
                 (client_id, temperature, humidity, light_intensity, fan_status, light_status, control_mode) 
                 VALUES (%s, %s, %s, %s, %s, %s, %s)"""
        val = (client_id, latest_reading['temperature'], latest_reading['humidity'], latest_reading['light_intensity'],
               latest_reading['fan_status'], latest_reading['light_status'], 'auto')
        
        cursor.execute(sql, val)
        conn.commit()
        return True
    except Exception as e:
        print(f"切换自动模式失败: {e}")
        conn.rollback()
        return False
    finally:
        if conn and conn.is_connected():
            cursor.close()
            conn.close()


@app.route('/')
def index():
    """Main page route, display sensor data"""
    conn = get_db_connection()
    if conn is None:
        return "Database connection failed", 500
    
    cursor = None
    try:
        cursor = conn.cursor(dictionary=True)
        # 使用窗口函数找出每个设备的最新记录
        cursor.execute("""
            WITH LatestReadings AS (
                SELECT *, ROW_NUMBER() OVER(PARTITION BY client_id ORDER BY timestamp DESC) as rn
                FROM sensor_readings
            )
            SELECT id, client_id, temperature, humidity, light_intensity, fan_status, light_status, timestamp, control_mode
            FROM LatestReadings
            WHERE rn <= 20  -- 每个设备最多显示20条记录
            ORDER BY timestamp DESC;
        """)
        all_readings = cursor.fetchall()
        
        # 找出每个设备的绝对最新一条记录的id，用于显示控制按钮
        latest_id_per_device = {}
        if all_readings:
            cursor.execute("""
                SELECT client_id, MAX(id) as max_id 
                FROM sensor_readings 
                GROUP BY client_id
            """)
            latest_ids_result = cursor.fetchall()
            latest_id_per_device = {row['client_id']: row['max_id'] for row in latest_ids_result}

        # 处理数据
        for reading in all_readings:
            reading['timestamp'] = reading['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            reading['temperature'] = float(reading['temperature'])
            reading['humidity'] = float(reading['humidity'])
            reading['light_intensity'] = float(reading['light_intensity'])
            reading['fan_status'] = bool(reading['fan_status'])
            reading['light_status'] = bool(reading['light_status'])
            reading['is_latest'] = (reading['id'] == latest_id_per_device.get(reading['client_id']))
            # 设置CSS类
            reading['temp_class'] = ''
            if reading['temperature'] >= 30.0: reading['temp_class'] = 'temperature-high'
            
            reading['light_class'] = ''
            if reading['light_intensity'] < 50.0: reading['light_class'] = 'light-low'
            
        return render_template('index.html', readings=all_readings)
    except Exception as e:
        print(f"An error occurred: {e}")
        return "An internal error occurred", 500
    finally:
        if conn and conn.is_connected():
            if cursor: cursor.close()
            conn.close()

@app.route('/control', methods=['POST'])
def control_device():
    """处理设备控制请求"""
    try:
        data = request.get_json()
        client_id = data.get('client_id')
        command = data.get('command')
        
        if not client_id or not command:
            return jsonify({'status': 'error', 'message': '缺少必要参数'}), 400
            
        # 发布MQTT命令
        command_topic = COMMAND_TOPIC_FORMAT.format(client_id=client_id)
        command_payload = json.dumps({"command": command, "timestamp": time.time()})
        mqtt_client.publish(command_topic, command_payload, qos=1)
        
        # 【修复】通过插入新记录来更新数据库状态
        if command == 'open_fan':
            set_device_manual_status(client_id, fan_status=1)
        elif command == 'close_fan':
            set_device_manual_status(client_id, fan_status=0)
        elif command == 'open_light':
            set_device_manual_status(client_id, light_status=1)
        elif command == 'close_light':
            set_device_manual_status(client_id, light_status=0)
        
        return jsonify({'status': 'success', 'message': '命令已发送'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/set_auto', methods=['POST'])
def set_auto():
    """切换设备回自动模式"""
    try:
        data = request.get_json()
        client_id = data.get('client_id')
        if not client_id:
            return jsonify({'status': 'error', 'message': '缺少必要参数'}), 400
        
        # 【修复】通过插入新记录切换到自动模式
        set_device_auto_mode(client_id)
        
        return jsonify({'status': 'success', 'message': '已切换为自动模式'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == '__main__':
    if not os.path.exists('templates'):
        os.makedirs('templates')
    
    # index.html内容保持不变，但修改了其中的JS逻辑
    with open('templates/index.html', 'w', encoding='utf-8') as f:
        f.write('''
<!DOCTYPE html>
<html lang="zh">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>IoT Sensor Data (Fixed)</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { padding: 20px; }
        .table-responsive { margin-top: 20px; }
        .temperature-high { color: #dc3545 !important; font-weight: bold; }
        .light-low { color: #6f42c1 !important; font-weight: bold; }
        .btn-control { width: 110px; margin: 2px; }
        .status-indicator { width: 15px; height: 15px; border-radius: 50%; display: inline-block; }
        .status-on { background-color: #198754; }
        .status-off { background-color: #6c757d; }
        .mode-manual { background-color: #ffc107; color: black; }
        .mode-auto { background-color: #0dcaf0; color: black; }
        .history-row td { color: #6c757d; opacity: 0.7; }
    </style>
</head>
<body>
    <div class="container-fluid">
        <h1 class="mb-4">IoT Sensor Data Dashboard</h1>
        <div class="table-responsive">
            <table class="table table-striped table-hover">
                <thead class="table-dark">
                    <tr>
                        <th>设备ID</th>
                        <th>温度 (°C)</th>
                        <th>湿度 (%)</th>
                        <th>光照 (lux)</th>
                        <th>风扇控制</th>
                        <th>补光灯控制</th>
                        <th>控制模式</th>
                        <th>时间戳</th>
                    </tr>
                </thead>
                <tbody>
                    {% for reading in readings %}
                    <tr class="{% if not reading.is_latest %}history-row{% endif %}">
                        <td>{{ reading.client_id }}</td>
                        <td class="{{ reading.temp_class }}">{{ "%.2f"|format(reading.temperature) }}</td>
                        <td>{{ "%.1f"|format(reading.humidity) }}</td>
                        <td class="{{ reading.light_class }}">{{ "%.1f"|format(reading.light_intensity) }}</td>
                        
                        {% if reading.is_latest %}
                        <td>
                            <div class="d-flex align-items-center">
                                <span class="status-indicator me-2 {% if reading.fan_status %}status-on{% else %}status-off{% endif %}"></span>
                                <button class="btn btn-sm btn-outline-primary btn-control fan-btn" 
                                        data-client-id="{{ reading.client_id }}"
                                        data-command="{{ 'close_fan' if reading.fan_status else 'open_fan' }}">
                                    {{ "关闭风扇" if reading.fan_status else "开启风扇" }}
                                </button>
                            </div>
                        </td>
                        <td>
                            <div class="d-flex align-items-center">
                                <span class="status-indicator me-2 {% if reading.light_status %}status-on{% else %}status-off{% endif %}"></span>
                                <button class="btn btn-sm btn-outline-primary btn-control light-btn" 
                                        data-client-id="{{ reading.client_id }}"
                                        data-command="{{ 'close_light' if reading.light_status else 'open_light' }}">
                                    {{ "关闭补光灯" if reading.light_status else "开启补光灯" }}
                                </button>
                            </div>
                        </td>
                        <td>
                            <span class="badge rounded-pill {{ 'mode-manual' if reading.control_mode == 'manual' else 'mode-auto' }}">
                                {{ '手动' if reading.control_mode == 'manual' else '自动' }}
                            </span>
                            {% if reading.control_mode == 'manual' %}
                            <button class="btn btn-sm btn-outline-success ms-2 auto-btn" data-client-id="{{ reading.client_id }}">恢复自动</button>
                            {% endif %}
                        </td>
                        {% else %}
                        <td><span class="badge bg-secondary">{{ '开启' if reading.fan_status else '关闭' }}</span></td>
                        <td><span class="badge bg-secondary">{{ '开启' if reading.light_status else '关闭' }}</span></td>
                        <td><span class="badge rounded-pill {{ 'mode-manual' if reading.control_mode == 'manual' else 'mode-auto' }}">{{ '手动' if reading.control_mode == 'manual' else '自动' }}</span></td>
                        {% endif %}

                        <td>{{ reading.timestamp }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
    </div>
    <script>
        function sendRequest(endpoint, body, reload_on_success = false) {
            fetch(endpoint, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(body)
            })
            .then(response => response.json())
            .then(data => {
                if(data.status === 'success') {
                    if(reload_on_success) {
                        window.location.reload();
                    }
                } else {
                    alert('操作失败: ' + data.message);
                }
            })
            .catch(error => {
                console.error('Error:', error);
                alert('请求失败，请检查控制台。');
            });
        }

        document.querySelectorAll('.fan-btn, .light-btn').forEach(btn => {
            btn.addEventListener('click', function() {
                const clientId = this.dataset.clientId;
                const command = this.dataset.command;
                sendRequest('/control', { client_id: clientId, command: command }, true);
            });
        });

        document.querySelectorAll('.auto-btn').forEach(btn => {
            btn.addEventListener('click', function() {
                const clientId = this.dataset.clientId;
                sendRequest('/set_auto', { client_id: clientId }, true);
            });
        });

        // 每30秒自动刷新页面
        setTimeout(() => window.location.reload(), 3000);
    </script>
</body>
</html>
        ''')
    
    app.run(host='0.0.0.0', port=5000)
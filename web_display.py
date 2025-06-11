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

def update_device_status(client_id, fan_status=None, light_status=None):
    """更新设备状态到数据库"""
    conn = get_db_connection()
    if conn is None:
        return False
    
    try:
        cursor = conn.cursor()
        if fan_status is not None and light_status is not None:
            sql = """UPDATE sensor_readings 
                    SET fan_status = %s, light_status = %s 
                    WHERE client_id = %s 
                    ORDER BY timestamp DESC LIMIT 1"""
            cursor.execute(sql, (fan_status, light_status, client_id))
        elif fan_status is not None:
            sql = """UPDATE sensor_readings 
                    SET fan_status = %s 
                    WHERE client_id = %s 
                    ORDER BY timestamp DESC LIMIT 1"""
            cursor.execute(sql, (fan_status, client_id))
        elif light_status is not None:
            sql = """UPDATE sensor_readings 
                    SET light_status = %s 
                    WHERE client_id = %s 
                    ORDER BY timestamp DESC LIMIT 1"""
            cursor.execute(sql, (light_status, client_id))
        
        conn.commit()
        return True
    except Exception as e:
        print(f"更新设备状态失败: {e}")
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
        cursor.execute("""
            SELECT id, client_id, temperature, humidity, light_intensity, fan_status, light_status, timestamp 
            FROM sensor_readings 
            ORDER BY timestamp DESC 
            LIMIT 200
        """)
        readings = cursor.fetchall()
        
        # 找出每个设备最新一条记录的id
        latest_id_per_device = {}
        for reading in readings:
            if reading['client_id'] not in latest_id_per_device:
                latest_id_per_device[reading['client_id']] = reading['id']
        
        # 处理数据
        for reading in readings:
            reading['timestamp'] = reading['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            reading['temperature'] = float(reading['temperature'])
            reading['humidity'] = float(reading['humidity'])
            reading['light_intensity'] = float(reading['light_intensity'])
            reading['fan_status'] = bool(reading['fan_status'])
            reading['light_status'] = bool(reading['light_status'])
            reading['is_latest'] = (reading['id'] == latest_id_per_device[reading['client_id']])
            # 设置CSS类
            reading['temp_class'] = ''
            reading['light_class'] = ''
            if reading['temperature'] >= 30.0:
                reading['temp_class'] = 'temperature-high'
            elif reading['temperature'] <= 25.0:
                reading['temp_class'] = 'temperature-low'
            if reading['light_intensity'] <= 50.0:
                reading['light_class'] = 'light-low'
        return render_template('index.html', readings=readings)
    except Exception as e:
        print(f"An error occurred: {e}")
        return "An internal error occurred", 500
    finally:
        if conn and conn.is_connected():
            if cursor:
                cursor.close()
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
        command_payload = json.dumps({
            "command": command,
            "timestamp": time.time()
        })
        
        # 发送MQTT命令
        mqtt_client.publish(command_topic, command_payload, qos=1)
        
        # 更新数据库状态
        if command == 'open_fan':
            update_device_status(client_id, fan_status=1)
        elif command == 'close_fan':
            update_device_status(client_id, fan_status=0)
        elif command == 'open_light':
            update_device_status(client_id, light_status=1)
        elif command == 'close_light':
            update_device_status(client_id, light_status=0)
        
        return jsonify({'status': 'success', 'message': '命令已发送'})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

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
        .light-low { color: #0d6efd !important; font-weight: bold; }
        .btn-control {
            width: 100px;
            margin: 2px;
        }
        .status-indicator {
            width: 20px;
            height: 20px;
            border-radius: 50%;
            display: inline-block;
            margin-right: 10px;
        }
        .status-on {
            background-color: #28a745;
        }
        .status-off {
            background-color: #dc3545;
        }
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
                        <th>Humidity (%)</th>
                        <th>Light Intensity</th>
                        <th>Fan Control</th>
                        <th>Light Control</th>
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
                        <td>{{ "%.1f"|format(reading.humidity) }}</td>
                        <td class="{{ reading.light_class }}">
                            {{ "%.1f"|format(reading.light_intensity) }}
                        </td>
                        {% if reading.is_latest %}
                        <td>
                            <div class="d-flex align-items-center">
                                <span class="status-indicator {% if reading.fan_status %}status-on{% else %}status-off{% endif %}"></span>
                                <button class="btn btn-sm btn-primary btn-control fan-btn" 
                                        data-client-id="{{ reading.client_id }}"
                                        data-status="{{ reading.fan_status|int }}">
                                    {{ "关闭风扇" if reading.fan_status else "开启风扇" }}
                                </button>
                            </div>
                        </td>
                        <td>
                            <div class="d-flex align-items-center">
                                <span class="status-indicator {% if reading.light_status %}status-on{% else %}status-off{% endif %}"></span>
                                <button class="btn btn-sm btn-primary btn-control light-btn" 
                                        data-client-id="{{ reading.client_id }}"
                                        data-status="{{ reading.light_status|int }}">
                                    {{ "关闭补光灯" if reading.light_status else "开启补光灯" }}
                                </button>
                            </div>
                        </td>
                        {% else %}
                        <td></td>
                        <td></td>
                        {% endif %}
                        <td>{{ reading.timestamp }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
    </div>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
    <script>
        // 处理风扇控制
        document.querySelectorAll('.fan-btn').forEach(btn => {
            btn.addEventListener('click', function() {
                const clientId = this.dataset.clientId;
                const currentStatus = parseInt(this.dataset.status);
                const command = currentStatus ? 'close_fan' : 'open_fan';
                sendCommand(clientId, command, this, 'fan');
            });
        });
        // 处理补光灯控制
        document.querySelectorAll('.light-btn').forEach(btn => {
            btn.addEventListener('click', function() {
                const clientId = this.dataset.clientId;
                const currentStatus = parseInt(this.dataset.status);
                const command = currentStatus ? 'close_light' : 'open_light';
                sendCommand(clientId, command, this, 'light');
            });
        });
        // 发送控制命令
        function sendCommand(clientId, command, button, type) {
            fetch('/control', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    client_id: clientId,
                    command: command
                })
            })
            .then(response => response.json())
            .then(data => {
                if (data.status === 'success') {
                    // 更新按钮状态
                    const newStatus = command.includes('open') ? 1 : 0;
                    button.dataset.status = newStatus;
                    button.textContent = newStatus ? 
                        (type === 'fan' ? '关闭风扇' : '关闭补光灯') : 
                        (type === 'fan' ? '开启风扇' : '开启补光灯');
                    // 更新状态指示器
                    const statusIndicator = button.parentElement.querySelector('.status-indicator');
                    statusIndicator.className = `status-indicator ${newStatus ? 'status-on' : 'status-off'}`;
                } else {
                    alert('控制命令发送失败：' + data.message);
                }
            })
            .catch(error => {
                console.error('Error:', error);
                alert('控制命令发送失败');
            });
        }
        // 自动刷新页面
        setTimeout(function() {
            window.location.reload();
        }, 30000);
    </script>
</body>
</html>
        ''')
    
    app.run(host='0.0.0.0', port=5000)
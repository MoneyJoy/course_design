# -*- coding: utf-8 -*-
from flask import Flask, render_template, request, jsonify, Response
import mysql.connector
from datetime import datetime
import os
import paho.mqtt.client as mqtt
import json
from decimal import Decimal
import time

app = Flask(__name__)

# --- 配置和非关键函数部分保持不变 ---
MYSQL_CONFIG = { 'host': 'localhost', 'user': 'm2joy', 'password': 'Liu041121@', 'database': 'iot_data' }
MQTT_BROKER_IP = 'localhost'
MQTT_BROKER_PORT = 1883
COMMAND_TOPIC_FORMAT = "stm32/command/{client_id}"

mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f"web_control_{int(time.time())}")
mqtt_client.connect(MQTT_BROKER_IP, MQTT_BROKER_PORT)
mqtt_client.loop_start()

def get_db_connection():
    try:
        return mysql.connector.connect(**MYSQL_CONFIG)
    except mysql.connector.Error as err:
        print(f"Database connection error: {err}")
        return None

# --- ▼▼▼ 核心修复区域 ▼▼▼ ---
def set_device_manual_status(client_id, fan_status=None, light_status=None):
    """
    当手动控制时，通过插入一条新的状态记录来确保手动操作成为最新状态。
    (已修复状态覆盖的Bug)
    """
    conn = get_db_connection()
    if conn is None: return False
    try:
        cursor = conn.cursor(dictionary=True)
        # 1. 获取最新的传感器读数和完整的设备状态
        cursor.execute("SELECT temperature, humidity, light_intensity, fan_status, light_status FROM sensor_readings WHERE client_id = %s ORDER BY timestamp DESC LIMIT 1", (client_id,))
        latest_reading = cursor.fetchone()
        if not latest_reading:
            # 如果没有历史记录，则假设初始状态都为False
            latest_reading = {'temperature': 0, 'humidity': 0, 'light_intensity': 0, 'fan_status': 0, 'light_status': 0}

        # 2. 正确计算风扇和灯的最终状态
        # 如果传入了新状态，则使用新状态；否则，保留从数据库读出的旧状态。
        new_fan_status = fan_status if fan_status is not None else latest_reading['fan_status']
        new_light_status = light_status if light_status is not None else latest_reading['light_status']
        
        # 3. 准备插入数据库
        sql = "INSERT INTO sensor_readings (client_id, temperature, humidity, light_intensity, fan_status, light_status, control_mode) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        
        # --- 核心BUG修复：使用计算好的新状态变量，而不是原始输入参数 ---
        val = (
            client_id,
            latest_reading['temperature'],
            latest_reading['humidity'],
            latest_reading['light_intensity'],
            new_fan_status,   # 使用 new_fan_status
            new_light_status, # 使用 new_light_status
            'manual'
        )
        
        cursor.execute(sql, val)
        conn.commit()
        return True
    except Exception as e:
        print(f"Error in set_device_manual_status: {e}")
        if conn and conn.is_connected(): conn.rollback()
        return False
    finally:
        if conn and conn.is_connected(): conn.close()
# --- ▲▲▲ 核心修复区域 ▲▲▲ ---

def set_device_auto_mode(client_id):
    conn = get_db_connection()
    if conn is None: return False
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM sensor_readings WHERE client_id = %s ORDER BY timestamp DESC LIMIT 1", (client_id,))
        latest_reading = cursor.fetchone()
        if not latest_reading: return False
        sql = "INSERT INTO sensor_readings (client_id, temperature, humidity, light_intensity, fan_status, light_status, control_mode) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (client_id, latest_reading['temperature'], latest_reading['humidity'], latest_reading['light_intensity'], latest_reading['fan_status'], latest_reading['light_status'], 'auto')
        cursor.execute(sql, val)
        conn.commit()
        return True
    finally:
        if conn and conn.is_connected(): conn.close()

@app.route('/')
def index():
    conn = get_db_connection()
    if conn is None: return "Database connection failed", 500
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("WITH LatestReadings AS (SELECT *, ROW_NUMBER() OVER(PARTITION BY client_id ORDER BY timestamp DESC) as rn FROM sensor_readings) SELECT * FROM LatestReadings WHERE rn <= 20 ORDER BY client_id, timestamp DESC;")
        all_readings = cursor.fetchall()
        latest_id_per_device = {}
        if all_readings:
            cursor.execute("SELECT client_id, MAX(id) as max_id FROM sensor_readings GROUP BY client_id")
            latest_ids_result = cursor.fetchall()
            latest_id_per_device = {row['client_id']: row['max_id'] for row in latest_ids_result}
        for reading in all_readings:
            reading['timestamp_str'] = reading['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            reading['temperature'] = float(reading['temperature'])
            reading['humidity'] = float(reading['humidity'])
            reading['light_intensity'] = float(reading['light_intensity'])
            reading['is_latest'] = (reading['id'] == latest_id_per_device.get(reading['client_id']))
            reading['temp_class'] = ''
            if reading['temperature'] >= 30.0: reading['temp_class'] = 'temperature-high'
            elif reading['temperature'] <= 25.0: reading['temp_class'] = 'temperature-low'
            reading['light_class'] = ''
            if reading['light_intensity'] < 50.0: reading['light_class'] = 'light-low'
        return render_template('index.html', readings=all_readings)
    finally:
        if conn and conn.is_connected(): conn.close()

@app.route('/control', methods=['POST'])
def control_device():
    data = request.get_json()
    client_id, command = data.get('client_id'), data.get('command')
    if not all([client_id, command]): return jsonify({'status': 'error', 'message': 'Missing parameters'}), 400
    command_topic = COMMAND_TOPIC_FORMAT.format(client_id=client_id)
    command_payload = json.dumps({"command": command, "timestamp": time.time()})
    mqtt_client.publish(command_topic, command_payload, qos=1)
    if command == 'open_fan': set_device_manual_status(client_id, fan_status=1)
    elif command == 'close_fan': set_device_manual_status(client_id, fan_status=0)
    elif command == 'open_light': set_device_manual_status(client_id, light_status=1)
    elif command == 'close_light': set_device_manual_status(client_id, light_status=0)
    return jsonify({'status': 'success'})

@app.route('/set_auto', methods=['POST'])
def set_auto():
    data = request.get_json()
    client_id = data.get('client_id')
    if not client_id: return jsonify({'status': 'error', 'message': 'Missing client_id'}), 400
    set_device_auto_mode(client_id)
    return jsonify({'status': 'success'})

@app.route('/stream')
def stream():
    def event_stream():
        last_id = 0
        try:
            conn_init = get_db_connection()
            if conn_init:
                with conn_init.cursor(dictionary=True) as cursor_init:
                    cursor_init.execute("SELECT MAX(id) as max_id FROM sensor_readings")
                    result = cursor_init.fetchone()
                    if result and result['max_id'] is not None:
                        last_id = result['max_id']
                conn_init.close()
        except Exception as e:
            print(f"Error getting initial max_id: {e}")

        try:
            while True:
                conn = get_db_connection()
                if conn:
                    try:
                        with conn.cursor(dictionary=True) as cursor:
                            query = "SELECT * FROM sensor_readings WHERE id > %s ORDER BY id ASC"
                            cursor.execute(query, (last_id,))
                            for reading in cursor.fetchall():
                                for key, value in reading.items():
                                    if isinstance(value, Decimal):
                                        reading[key] = float(value)
                                    elif isinstance(value, datetime):
                                        reading[key] = value.isoformat()
                                
                                yield f"data: {json.dumps(reading)}\n\n"
                                last_id = reading['id']
                    finally:
                        conn.close()
                time.sleep(1)
        except GeneratorExit:
            print("Client disconnected, closing stream.")

    return Response(event_stream(), mimetype='text/event-stream')


if __name__ == '__main__':
    if not os.path.exists('templates'):
        os.makedirs('templates')
    
    with open('templates/index.html', 'w', encoding='utf-8') as f:
        # 前端 HTML 和 JavaScript 无需改动
        f.write('''
<!DOCTYPE html>
<html lang="zh">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>IoT Real-Time Sensor Data</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { padding: 20px; } .table-responsive { margin-top: 20px; }
        .temperature-high { color: #dc3545 !important; font-weight: bold; }
        .temperature-low { color: #0d6efd !important; font-weight: bold; }
        .light-low { color: #6f42c1 !important; font-weight: bold; }
        .btn-control { width: 110px; margin: 2px; }
        .status-indicator { width: 15px; height: 15px; border-radius: 50%; display: inline-block; }
        .status-on { background-color: #198754; } .status-off { background-color: #6c757d; }
        .mode-manual { background-color: #ffc107; color: black; }
        .mode-auto { background-color: #0dcaf0; color: black; }
        .history-row td { color: #6c757d; opacity: 0.7; }
        .new-row { background-color: #d1e7dd !important; transition: background-color 1s ease-out; }
    </style>
</head>
<body>
    <div class="container-fluid">
        <h1 class="mb-4">IoT 实时传感器数据面板</h1>
        <div class="table-responsive">
            <table class="table table-striped table-hover">
                <thead class="table-dark">
                    <tr><th>设备ID</th><th>温度 (°C)</th><th>湿度 (%)</th><th>光照 (lux)</th><th>风扇控制</th><th>补光灯控制</th><th>控制模式</th><th>时间戳</th></tr>
                </thead>
                <tbody id="data-table-body">
                    {% for reading in readings %}
                    <tr class="{% if not reading.is_latest %}history-row{% endif %}" data-client-id="{{ reading.client_id }}" data-record-id="{{ reading.id }}">
                        <td>{{ reading.client_id }}</td>
                        <td class="temp-cell {{ reading.temp_class }}">{{ "%.2f"|format(reading.temperature) }}</td>
                        <td class="hum-cell">{{ "%.1f"|format(reading.humidity) }}</td>
                        <td class="light-cell {{ reading.light_class }}">{{ "%.1f"|format(reading.light_intensity) }}</td>
                        <td class="fan-control-cell">
                            {% if reading.is_latest %}
                            <div class="d-flex align-items-center">
                                <span class="status-indicator me-2 {% if reading.fan_status %}status-on{% else %}status-off{% endif %}"></span>
                                <button class="btn btn-sm btn-outline-primary btn-control fan-btn" data-command="{{ 'close_fan' if reading.fan_status else 'open_fan' }}">{{ "关闭风扇" if reading.fan_status else "开启风扇" }}</button>
                            </div>
                            {% else %}<span class="badge bg-secondary">{{ '开启' if reading.fan_status else '关闭' }}</span>{% endif %}
                        </td>
                        <td class="light-control-cell">
                             {% if reading.is_latest %}
                            <div class="d-flex align-items-center">
                                <span class="status-indicator me-2 {% if reading.light_status %}status-on{% else %}status-off{% endif %}"></span>
                                <button class="btn btn-sm btn-outline-primary btn-control light-btn" data-command="{{ 'close_light' if reading.light_status else 'open_light' }}">{{ "关闭补光灯" if reading.light_status else "开启补光灯" }}</button>
                            </div>
                            {% else %}<span class="badge bg-secondary">{{ '开启' if reading.light_status else '关闭' }}</span>{% endif %}
                        </td>
                        <td class="mode-cell">
                            <span class="badge rounded-pill {{ 'mode-manual' if reading.control_mode == 'manual' else 'mode-auto' }}">{{ '手动' if reading.control_mode == 'manual' else '自动' }}</span>
                            {% if reading.is_latest and reading.control_mode == 'manual' %}<button class="btn btn-sm btn-outline-success ms-2 auto-btn">恢复自动</button>{% endif %}
                        </td>
                        <td class="timestamp-cell">{{ reading.timestamp_str }}</td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
    </div>
    
    <script>
        document.addEventListener('DOMContentLoaded', function() {
            const tableBody = document.getElementById('data-table-body');
            
            tableBody.addEventListener('click', function(event) {
                const button = event.target.closest('button');
                if (!button) return;
                const row = button.closest('tr');
                if (!row) return;
                const clientId = row.dataset.clientId;
                if (button.classList.contains('fan-btn') || button.classList.contains('light-btn')) {
                    sendRequest('/control', { client_id: clientId, command: button.dataset.command });
                } else if (button.classList.contains('auto-btn')) {
                    sendRequest('/set_auto', { client_id: clientId });
                }
            });

            const eventSource = new EventSource("/stream");
            eventSource.onmessage = function(event) {
                try {
                    const newData = JSON.parse(event.data);
                    if (newData.error) { return; }
                    console.log("新数据已接收:", newData);
                    updateTable(newData);
                } catch (e) { console.error("解析JSON失败:", e, "收到的数据:", event.data); }
            };
            eventSource.onerror = function(err) { console.error("EventSource 失败:", err); };
        });

        function updateTable(data) {
            const tableBody = document.getElementById('data-table-body');
            const oldLatestRow = tableBody.querySelector(`tr[data-client-id="${data.client_id}"]:not(.history-row)`);

            if (oldLatestRow) {
                oldLatestRow.classList.add('history-row');
                const fanWasOn = oldLatestRow.querySelector('.fan-control-cell .status-indicator')?.classList.contains('status-on');
                oldLatestRow.querySelector('.fan-control-cell').innerHTML = `<span class="badge bg-secondary">${fanWasOn ? '开启' : '关闭'}</span>`;
                const lightWasOn = oldLatestRow.querySelector('.light-control-cell .status-indicator')?.classList.contains('status-on');
                oldLatestRow.querySelector('.light-control-cell').innerHTML = `<span class="badge bg-secondary">${lightWasOn ? '开启' : '关闭'}</span>`;
                oldLatestRow.querySelector('.mode-cell button')?.remove();
            }

            const newRow = document.createElement('tr');
            newRow.dataset.clientId = data.client_id; newRow.dataset.recordId = data.id;
            newRow.classList.add('new-row');
            const tempClass = parseFloat(data.temperature) >= 30 ? 'temperature-high' : (parseFloat(data.temperature) <= 25 ? 'temperature-low' : '');
            const lightClass = parseFloat(data.light_intensity) < 50 ? 'light-low' : '';
            const fanControlHtml = `<div class="d-flex align-items-center"><span class="status-indicator me-2 ${data.fan_status ? 'status-on' : 'status-off'}"></span><button class="btn btn-sm btn-outline-primary btn-control fan-btn" data-command="${data.fan_status ? 'close_fan' : 'open_fan'}">${data.fan_status ? '关闭风扇' : '开启风扇'}</button></div>`;
            const lightControlHtml = `<div class="d-flex align-items-center"><span class="status-indicator me-2 ${data.light_status ? 'status-on' : 'status-off'}"></span><button class="btn btn-sm btn-outline-primary btn-control light-btn" data-command="${data.light_status ? 'close_light' : 'open_light'}">${data.light_status ? '关闭补光灯' : '开启补光灯'}</button></div>`;
            let modeHtml = `<span class="badge rounded-pill ${data.control_mode === 'manual' ? 'mode-manual' : 'mode-auto'}">${data.control_mode === 'manual' ? '手动' : '自动'}</span>`;
            if (data.control_mode === 'manual') { modeHtml += ` <button class="btn btn-sm btn-outline-success ms-2 auto-btn">恢复自动</button>`; }
            const timestamp = new Date(data.timestamp).toLocaleString();
            newRow.innerHTML = `<td>${data.client_id}</td><td class="temp-cell ${tempClass}">${parseFloat(data.temperature).toFixed(2)}</td><td class="hum-cell">${parseFloat(data.humidity).toFixed(1)}</td><td class="light-cell ${lightClass}">${parseFloat(data.light_intensity).toFixed(1)}</td><td class="fan-control-cell">${fanControlHtml}</td><td class="light-control-cell">${lightControlHtml}</td><td class="mode-cell">${modeHtml}</td><td class="timestamp-cell">${timestamp}</td>`;
            tableBody.prepend(newRow);
            setTimeout(() => newRow.classList.remove('new-row'), 1000);
        }
        
        function sendRequest(endpoint, body) {
            fetch(endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
            .then(response => response.json())
            .then(data => { if(data.status !== 'success' && data.message) { alert('操作失败: ' + data.message); }})
            .catch(error => console.error('Error:', error));
        }
    </script>
</body>
</html>
        ''')
    
    app.run(host='0.0.0.0', port=5000, debug=True)
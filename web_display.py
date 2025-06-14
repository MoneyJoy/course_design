# -*- coding: utf-8 -*-
"""
Web显示和控制服务 (最终稳定版 v2.2)
=====================================
- 实时通信: 集成Redis和SSE
- 可视化: 集成ECharts实时折线图
- Bug修复: 修复所有已知问题
"""
from flask import Flask, render_template, request, jsonify, Response
import mysql.connector
from datetime import datetime
import os
import paho.mqtt.client as mqtt
import json
from decimal import Decimal
import time
import redis

app = Flask(__name__)

# --- 配置 ---
MYSQL_CONFIG = { 'host': 'localhost', 'user': 'm2joy', 'password': 'Liu041121@', 'database': 'iot_data' }
MQTT_BROKER_IP = 'localhost'
MQTT_BROKER_PORT = 1883
COMMAND_TOPIC_FORMAT = "stm32/command/{client_id}"
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_CHANNEL = 'iot_data_stream'

# --- 客户端和服务初始化 ---
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, f"web_control_{int(time.time())}")
mqtt_client.connect(MQTT_BROKER_IP, MQTT_BROKER_PORT)
mqtt_client.loop_start()

def get_db_connection():
    try:
        return mysql.connector.connect(**MYSQL_CONFIG)
    except mysql.connector.Error as err:
        print(f"Database connection error: {err}")
        return None

def create_table_if_not_exists():
    """启动时检查并创建数据表"""
    conn = get_db_connection()
    if not conn:
        print("❌ 无法连接到数据库，跳过表创建检查。")
        return
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sensor_readings (
                    id INT AUTO_INCREMENT PRIMARY KEY, client_id VARCHAR(255) NOT NULL,
                    temperature DECIMAL(5,2) NOT NULL, humidity DECIMAL(4,1) NOT NULL,
                    light_intensity DECIMAL(7,1) NOT NULL, fan_status TINYINT(1) DEFAULT 0,
                    light_status TINYINT(1) DEFAULT 0, control_mode VARCHAR(10) DEFAULT 'auto',
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
        conn.commit()
    finally:
        if conn.is_connected(): conn.close()

def publish_to_redis(record_dict):
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        for key, value in record_dict.items():
            if isinstance(value, datetime): record_dict[key] = value.isoformat()
            elif isinstance(value, Decimal): record_dict[key] = float(value)
        r.publish(REDIS_CHANNEL, json.dumps(record_dict))
        r.close()
    except Exception as e:
        print(f"❌ Web App发布到Redis失败: {e}")

def set_device_manual_status(client_id, fan_status=None, light_status=None):
    conn = get_db_connection()
    if conn is None: return
    try:
        with conn.cursor(dictionary=True) as cursor:
            cursor.execute("SELECT temperature, humidity, light_intensity, fan_status, light_status FROM sensor_readings WHERE client_id = %s ORDER BY timestamp DESC LIMIT 1", (client_id,))
            latest_reading = cursor.fetchone() or {'temperature': 0, 'humidity': 0, 'light_intensity': 0, 'fan_status': 0, 'light_status': 0}
            new_fan_status = fan_status if fan_status is not None else latest_reading['fan_status']
            new_light_status = light_status if light_status is not None else latest_reading['light_status']
            sql = "INSERT INTO sensor_readings (client_id, temperature, humidity, light_intensity, fan_status, light_status, control_mode) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            val = (client_id, latest_reading['temperature'], latest_reading['humidity'], latest_reading['light_intensity'], new_fan_status, new_light_status, 'manual')
            cursor.execute(sql, val)
            new_id = cursor.lastrowid
            conn.commit()
            cursor.execute("SELECT * FROM sensor_readings WHERE id = %s", (new_id,))
            new_record = cursor.fetchone()
            if new_record: publish_to_redis(new_record)
    finally:
        if conn.is_connected(): conn.close()

def set_device_auto_mode(client_id):
    conn = get_db_connection()
    if conn is None: return
    try:
        with conn.cursor(dictionary=True) as cursor:
            cursor.execute("SELECT * FROM sensor_readings WHERE client_id = %s ORDER BY timestamp DESC LIMIT 1", (client_id,))
            latest_reading = cursor.fetchone()
            if not latest_reading: return
            sql = "INSERT INTO sensor_readings (client_id, temperature, humidity, light_intensity, fan_status, light_status, control_mode) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            val = (client_id, latest_reading['temperature'], latest_reading['humidity'], latest_reading['light_intensity'], latest_reading['fan_status'], latest_reading['light_status'], 'auto')
            cursor.execute(sql, val)
            new_id = cursor.lastrowid
            conn.commit()
            cursor.execute("SELECT * FROM sensor_readings WHERE id = %s", (new_id,))
            new_record = cursor.fetchone()
            if new_record: publish_to_redis(new_record)
    finally:
        if conn.is_connected(): conn.close()

@app.route('/')
def index():
    conn = get_db_connection()
    if conn is None: return "Database connection failed", 500
    all_readings, chart_data = [], {}
    try:
        with conn.cursor(dictionary=True) as cursor:
            cursor.execute("SELECT DISTINCT client_id FROM sensor_readings ORDER BY client_id")
            devices = [row['client_id'] for row in cursor]
            for device_id in devices:
                query = ("(SELECT timestamp, temperature, humidity, light_intensity FROM sensor_readings WHERE client_id = %s ORDER BY timestamp DESC LIMIT 30) ORDER BY timestamp ASC;")
                cursor.execute(query, (device_id,))
                records = cursor.fetchall()
                chart_data[device_id] = {'labels': [r['timestamp'].strftime('%H:%M:%S') for r in records],'temperatures': [float(r['temperature']) for r in records],'humidities': [float(r['humidity']) for r in records],'lights': [float(r['light_intensity']) for r in records]}
            
            cursor.execute("WITH LatestReadings AS (SELECT *, ROW_NUMBER() OVER(PARTITION BY client_id ORDER BY timestamp DESC) as rn FROM sensor_readings) SELECT * FROM LatestReadings WHERE rn <= 20 ORDER BY client_id, timestamp DESC;")
            all_readings = cursor.fetchall()
            
            cursor.execute("SELECT client_id, MAX(id) as max_id FROM sensor_readings GROUP BY client_id")
            latest_id_per_device = {row['client_id']: row['max_id'] for row in cursor}
            
            for reading in all_readings:
                reading['timestamp_str'] = reading['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
                reading['temperature'], reading['humidity'], reading['light_intensity'] = float(reading['temperature']), float(reading['humidity']), float(reading['light_intensity'])
                reading['is_latest'] = (reading['id'] == latest_id_per_device.get(reading['client_id']))
                reading['temp_class'] = 'temperature-high' if reading['temperature'] >= 30.0 else ('temperature-low' if reading['temperature'] <= 25.0 else '')
                reading['light_class'] = 'light-low' if reading['light_intensity'] < 50.0 else ''
        return render_template('index.html', readings=all_readings, chart_data=json.dumps(chart_data))
    finally:
        if conn.is_connected(): conn.close()

@app.route('/control', methods=['POST'])
def control_device():
    data = request.get_json(); client_id, command = data.get('client_id'), data.get('command')
    if not all([client_id, command]): return jsonify({'status': 'error', 'message': 'Missing parameters'}), 400
    command_topic = COMMAND_TOPIC_FORMAT.format(client_id=client_id); command_payload = json.dumps({"command": command, "timestamp": time.time()})
    mqtt_client.publish(command_topic, command_payload, qos=1)
    if command == 'open_fan': set_device_manual_status(client_id, fan_status=1)
    elif command == 'close_fan': set_device_manual_status(client_id, fan_status=0)
    elif command == 'open_light': set_device_manual_status(client_id, light_status=1)
    elif command == 'close_light': set_device_manual_status(client_id, light_status=0)
    return jsonify({'status': 'success'})

@app.route('/set_auto', methods=['POST'])
def set_auto():
    data = request.get_json(); client_id = data.get('client_id')
    if not client_id: return jsonify({'status': 'error', 'message': 'Missing client_id'}), 400
    set_device_auto_mode(client_id); return jsonify({'status': 'success'})

@app.route('/stream')
def stream():
    def event_stream():
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True); pubsub = r.pubsub(); pubsub.subscribe(REDIS_CHANNEL)
        print(f"✅ 新Web客户端已订阅Redis频道 '{REDIS_CHANNEL}'");
        try:
            for message in pubsub.listen():
                if message['type'] == 'message': yield f"data: {message['data']}\n\n"
        finally: pubsub.close(); r.close(); print(f"❌ Web客户端断开连接，取消订阅。")
    return Response(event_stream(), mimetype='text/event-stream')

if __name__ == '__main__':
    create_table_if_not_exists() # 启动时确保表存在
    if not os.path.exists('templates'): os.makedirs('templates')
    with open('templates/index.html', 'w', encoding='utf-8') as f:
        # 包含了ECharts图表功能的最终HTML模板
        f.write('''
<!DOCTYPE html>
<html lang="zh">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>IoT Real-Time Dashboard with Charts</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/echarts@5.5.0/dist/echarts.min.js"></script>
    <style>
        body { padding: 20px; background-color: #f8f9fa; }
        .chart-container { width: 100%; height: 400px; background-color: #ffffff; border-radius: 8px; padding: 15px; margin-bottom: 25px; box-shadow: 0 4px 8px rgba(0,0,0,0.05); }
        .table-responsive { margin-top: 20px; }
        .temperature-high { color: #dc3545 !important; font-weight: bold; }
        .temperature-low { color: #0d6efd !important; font-weight: bold; }
        .light-low { color: #6f42c1 !important; font-weight: bold; }
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
        <h1 class="mb-4 text-center">IoT 实时数据监控面板</h1>
        <div id="charts-area"></div>
        <h2 class="mt-5 mb-3">实时数据表格</h2>
        <div class="table-responsive">
            <table class="table table-striped table-hover">
                <thead class="table-dark"><tr><th>设备ID</th><th>温度 (°C)</th><th>湿度 (%)</th><th>光照 (lux)</th><th>风扇控制</th><th>补光灯控制</th><th>控制模式</th><th>时间戳</th></tr></thead>
                <tbody id="data-table-body">
                {% if readings %}{% for reading in readings %}<tr class="{% if not reading.is_latest %}history-row{% endif %}" data-client-id="{{ reading.client_id }}" data-record-id="{{ reading.id }}"><td>{{ reading.client_id }}</td><td class="temp-cell {{ reading.temp_class }}">{{ "%.2f"|format(reading.temperature) }}</td><td class="hum-cell">{{ "%.1f"|format(reading.humidity) }}</td><td class="light-cell {{ reading.light_class }}">{{ "%.1f"|format(reading.light_intensity) }}</td><td class="fan-control-cell">{% if reading.is_latest %}<div class="d-flex align-items-center"><span class="status-indicator me-2 {% if reading.fan_status %}status-on{% else %}status-off{% endif %}"></span><button class="btn btn-sm btn-outline-primary btn-control fan-btn" data-command="{{ 'close_fan' if reading.fan_status else 'open_fan' }}">{{ "关闭风扇" if reading.fan_status else "开启风扇" }}</button></div>{% else %}<span class="badge bg-secondary">{{ '开启' if reading.fan_status else '关闭' }}</span>{% endif %}</td><td class="light-control-cell">{% if reading.is_latest %}<div class="d-flex align-items-center"><span class="status-indicator me-2 {% if reading.light_status %}status-on{% else %}status-off{% endif %}"></span><button class="btn btn-sm btn-outline-primary btn-control light-btn" data-command="{{ 'close_light' if reading.light_status else 'open_light' }}">{{ "关闭补光灯" if reading.light_status else "开启补光灯" }}</button></div>{% else %}<span class="badge bg-secondary">{{ '开启' if reading.light_status else '关闭' }}</span>{% endif %}</td><td class="mode-cell"><span class="badge rounded-pill {{ 'mode-manual' if reading.control_mode == 'manual' else 'mode-auto' }}">{{ '手动' if reading.control_mode == 'manual' else '自动' }}</span>{% if reading.is_latest and reading.control_mode == 'manual' %}<button class="btn btn-sm btn-outline-success ms-2 auto-btn">恢复自动</button>{% endif %}</td><td class="timestamp-cell">{{ reading.timestamp_str }}</td></tr>{% endfor %}{% endif %}
                </tbody>
            </table>
        </div>
    </div>
    <script>
        const initialChartData = JSON.parse({{ chart_data | tojson | safe }});
        const chartInstances = {};
        document.addEventListener('DOMContentLoaded', function() {
            const chartsArea = document.getElementById('charts-area');
            for (const deviceId in initialChartData) {
                const chartWrapper = document.createElement('div');
                chartWrapper.innerHTML = `<h2>设备 ${deviceId} - 实时数据图表</h2>`;
                const chartDom = document.createElement('div');
                chartDom.id = `chart-${deviceId}`; chartDom.className = 'chart-container';
                chartWrapper.appendChild(chartDom); chartsArea.appendChild(chartWrapper);
                const myChart = echarts.init(chartDom);
                const data = initialChartData[deviceId];
                myChart.setOption({tooltip: { trigger: 'axis' },legend: { data: ['温度', '湿度', '光照强度'] },grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },xAxis: { type: 'category', boundaryGap: false, data: data.labels },yAxis: [ { type: 'value', name: '温度/湿度' }, { type: 'value', name: '光照', position: 'right' } ],series: [{ name: '温度', type: 'line', smooth: true, data: data.temperatures },{ name: '湿度', type: 'line', smooth: true, data: data.humidities },{ name: '光照强度', type: 'line', smooth: true, yAxisIndex: 1, data: data.lights }]});
                chartInstances[deviceId] = myChart;
            }
            const eventSource = new EventSource("/stream");
            eventSource.onmessage = function(event) {
                const newData = JSON.parse(event.data);
                if (newData.error) return;
                console.log("新数据已接收:", newData);
                updateTable(newData); updateChart(newData);
            };
            const tableBody = document.getElementById('data-table-body');
            tableBody.addEventListener('click', function(event) {
                const button = event.target.closest('button'); if (!button) return;
                const row = button.closest('tr'); if (!row) return;
                const clientId = row.dataset.clientId;
                if (button.classList.contains('fan-btn') || button.classList.contains('light-btn')) {
                    sendRequest('/control', { client_id: clientId, command: button.dataset.command });
                } else if (button.classList.contains('auto-btn')) { sendRequest('/set_auto', { client_id: clientId }); }
            });
        });
        function updateChart(data) {
            let chart = chartInstances[data.client_id];
            if (!chart) { /* 如果是新设备，则动态创建图表 */ return; }
            const option = chart.getOption();
            const [labels, temps, hums, lights] = [option.xAxis[0].data, option.series[0].data, option.series[1].data, option.series[2].data];
            labels.push(new Date(data.timestamp).toLocaleTimeString()); temps.push(data.temperature); hums.push(data.humidity); lights.push(data.light_intensity);
            if (labels.length > 30) { labels.shift(); temps.shift(); hums.shift(); lights.shift(); }
            chart.setOption({xAxis: { data: labels },series: [{ data: temps }, { data: hums }, { data: lights }]});
        }
        function updateTable(data) {
            const tableBody = document.getElementById('data-table-body');
            let rowToUpdate = tableBody.querySelector(`tr[data-client-id="${data.client_id}"]:not(.history-row)`);
            if(rowToUpdate) {
                rowToUpdate.classList.add('history-row');
                rowToUpdate.querySelector('.fan-control-cell').innerHTML = `<span class="badge bg-secondary">${rowToUpdate.querySelector('.fan-control-cell .status-indicator')?.classList.contains('status-on') ? '开启' : '关闭'}</span>`;
                rowToUpdate.querySelector('.light-control-cell').innerHTML = `<span class="badge bg-secondary">${rowToUpdate.querySelector('.light-control-cell .status-indicator')?.classList.contains('status-on') ? '开启' : '关闭'}</span>`;
                rowToUpdate.querySelector('.mode-cell button')?.remove();
            }
            const newRow = document.createElement('tr');
            newRow.dataset.clientId = data.client_id; newRow.dataset.recordId = data.id; newRow.classList.add('new-row');
            const tempClass = parseFloat(data.temperature) >= 30 ? 'temperature-high' : (parseFloat(data.temperature) <= 25 ? 'temperature-low' : '');
            const lightClass = parseFloat(data.light_intensity) < 50 ? 'light-low' : '';
            const fanCtrl = `<div class="d-flex align-items-center"><span class="status-indicator me-2 ${data.fan_status ? 'status-on' : 'status-off'}"></span><button class="btn btn-sm btn-outline-primary fan-btn" data-command="${data.fan_status ? 'close_fan' : 'open_fan'}">${data.fan_status ? '关闭风扇' : '开启风扇'}</button></div>`;
            const lightCtrl = `<div class="d-flex align-items-center"><span class="status-indicator me-2 ${data.light_status ? 'status-on' : 'status-off'}"></span><button class="btn btn-sm btn-outline-primary light-btn" data-command="${data.light_status ? 'close_light' : 'open_light'}">${data.light_status ? '关闭补光灯' : '开启补光灯'}</button></div>`;
            let modeHtml = `<span class="badge rounded-pill ${data.control_mode === 'manual' ? 'mode-manual' : 'mode-auto'}">${data.control_mode === 'manual' ? '手动' : '自动'}</span>`;
            if (data.control_mode === 'manual') modeHtml += ` <button class="btn btn-sm btn-outline-success ms-2 auto-btn">恢复自动</button>`;
            newRow.innerHTML = `<td>${data.client_id}</td><td class="temp-cell ${tempClass}">${parseFloat(data.temperature).toFixed(2)}</td><td class="hum-cell">${parseFloat(data.humidity).toFixed(1)}</td><td class="light-cell ${lightClass}">${parseFloat(data.light_intensity).toFixed(1)}</td><td class="fan-control-cell">${fanCtrl}</td><td class="light-control-cell">${lightCtrl}</td><td class="mode-cell">${modeHtml}</td><td class="timestamp-cell">${new Date(data.timestamp).toLocaleString()}</td>`;
            tableBody.prepend(newRow);
            setTimeout(() => newRow.classList.remove('new-row'), 1000);
        }
        function sendRequest(endpoint, body) {
            fetch(endpoint, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) })
            .then(res => res.json()).then(data => { if(data.status !== 'success' && data.message) alert('操作失败: ' + data.message); })
            .catch(error => console.error('Error:', error));
        }
    </script>
</body>
</html>''')
    
    app.run(host='0.0.0.0', port=5000, debug=True)
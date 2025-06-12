# -*- coding: utf-8 -*-
"""
MQTT物联网网关服务 (最终稳定版 v2.0)
=====================================
- 状态管理: 数据库作为唯一可信源
- 实时通信: 集成Redis发布/订阅模式，实现高效消息通知
"""
import paho.mqtt.client as mqtt
import json
import mysql.connector
from mysql.connector import Error
import time
import random
import redis
from datetime import datetime
from decimal import Decimal

# --- 配置信息 ---
MQTT_BROKER_IP = 'localhost'
MQTT_BROKER_PORT = 1883
MQTT_TIMEOUT = 60
DATA_TOPIC = "stm32/data"
COMMAND_TOPIC_FORMAT = "stm32/command/{client_id}"

MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'm2joy',
    'password': 'Liu041121@',
    'database': 'iot_data'
}

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_CHANNEL = 'iot_data_stream'


class MqttGateway:
    def __init__(self, broker_ip, port, timeout):
        self.broker_ip = broker_ip
        self.broker_port = port
        self.timeout = timeout
        self.client = None
        
        try:
            self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
            self.redis_client.ping()
            print("✅ 成功连接到Redis服务器")
        except redis.exceptions.ConnectionError as e:
            print(f"❌ 连接Redis失败: {e}")
            self.redis_client = None

        self.start_client()

    def get_db_connection(self):
        try:
            return mysql.connector.connect(**MYSQL_CONFIG)
        except Error as e:
            print(f"❌ 数据库连接失败: {e}")
            return None

    def start_client(self):
        client_name = f"mqtt_gateway_{random.randint(1000, 9999)}"
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_name)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        try:
            self.client.connect(self.broker_ip, self.broker_port, self.timeout)
            self.client.loop_start()
        except Exception as e:
            print(f"❌ 连接MQTT代理服务器失败: {e}")

    def on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            print("✅ 成功连接到MQTT代理服务器!")
            client.subscribe(DATA_TOPIC)
            print(f"📡 已订阅主题: {DATA_TOPIC}")
        else:
            print(f"❌ 连接失败，错误代码: {rc}")

    def get_latest_device_status(self, client_id):
        conn = self.get_db_connection()
        if not conn: return None
        try:
            with conn.cursor(dictionary=True) as cursor:
                query = "SELECT fan_status, light_status, control_mode FROM sensor_readings WHERE client_id = %s ORDER BY timestamp DESC LIMIT 1"
                cursor.execute(query, (client_id,))
                return cursor.fetchone()
        finally:
            if conn.is_connected(): conn.close()

    def publish_to_redis(self, record_dict):
        if not self.redis_client: return
        try:
            # 预处理数据类型以确保JSON兼容性
            for key, value in record_dict.items():
                if isinstance(value, datetime):
                    record_dict[key] = value.isoformat()
                elif isinstance(value, Decimal):
                    record_dict[key] = float(value)
            
            self.redis_client.publish(REDIS_CHANNEL, json.dumps(record_dict))
            print(f"📡 已将记录ID {record_dict.get('id')} 发布到Redis频道 '{REDIS_CHANNEL}'")
        except Exception as e:
            print(f"❌ 发布到Redis失败: {e}")

    def on_message(self, client, userdata, msg):
        try:
            payload_str = msg.payload.decode('utf-8')
            print(f"📨 收到消息: {payload_str}")

            content = payload_str.strip('{}')
            parts = content.split(';')
            if len(parts) != 4: return

            client_id, temp_str, hum_str, light_str = parts
            temperature, humidity, light_intensity = float(temp_str), float(hum_str), float(light_str)

            latest_status = self.get_latest_device_status(client_id)
            db_fan_status = bool(latest_status['fan_status']) if latest_status else False
            db_light_status = bool(latest_status['light_status']) if latest_status else False
            db_control_mode = latest_status['control_mode'] if latest_status else 'auto'

            new_fan_state, new_light_state, new_mode = db_fan_status, db_light_status, db_control_mode

            if db_control_mode == 'auto':
                if temperature >= 30.0 and not db_fan_status:
                    self.publish_command(client_id, "open_fan")
                    new_fan_state = True
                elif temperature <= 25.0 and db_fan_status:
                    self.publish_command(client_id, "close_fan")
                    new_fan_state = False
                
                if light_intensity < 50.0 and not db_light_status:
                    self.publish_command(client_id, "open_light")
                    new_light_state = True
                elif light_intensity >= 50.0 and db_light_status:
                    self.publish_command(client_id, "close_light")
                    new_light_state = False
            
            # 统一的数据保存和发布流程
            conn = self.get_db_connection()
            if not conn: return
            try:
                with conn.cursor(dictionary=True) as cursor:
                    sql = """INSERT INTO sensor_readings (client_id, temperature, humidity, light_intensity, fan_status, light_status, control_mode) 
                             VALUES (%s, %s, %s, %s, %s, %s, %s)"""
                    val = (client_id, temperature, humidity, light_intensity, 1 if new_fan_state else 0, 1 if new_light_state else 0, new_mode)
                    cursor.execute(sql, val)
                    new_id = cursor.lastrowid # 获取新插入记录的ID
                    conn.commit()
                    print(f"💾 数据已保存到数据库, ID={new_id}")
                    
                    # 获取完整的新记录并发布到Redis
                    cursor.execute("SELECT * FROM sensor_readings WHERE id = %s", (new_id,))
                    new_record = cursor.fetchone()
                    if new_record:
                        self.publish_to_redis(new_record)
            finally:
                if conn.is_connected(): conn.close()

        except Exception as e:
            print(f"❌ 处理消息时发生未知错误: {e}")

    def publish_command(self, client_id, command):
        command_topic = COMMAND_TOPIC_FORMAT.format(client_id=client_id)
        command_payload = json.dumps({"command": command, "timestamp": time.time()})
        self.client.publish(command_topic, command_payload, qos=1)
        print(f"📤 已发送指令 '{command}' 到 '{command_topic}'")

if __name__ == '__main__':
    gateway = MqttGateway(MQTT_BROKER_IP, MQTT_BROKER_PORT, MQTT_TIMEOUT)
    print("🚀 MQTT物联网网关服务已启动 (v2.0 - Redis集成版)")
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        if gateway.client: gateway.client.loop_stop()
        print("\n✅ 网关服务已安全关闭")
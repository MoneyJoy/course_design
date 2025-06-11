# -*- coding: utf-8 -*-
"""
MQTT物联网网关服务
====================

这是一个基于Python的MQTT网关服务，主要功能包括：
1. 连接MQTT代理服务器（如EMQX）
2. 订阅设备数据主题，接收来自STM32等设备的传感器数据
3. 将接收到的数据存储到MySQL数据库中
4. 根据业务逻辑（如温度阈值）向设备下发控制指令
5. 提供完整的错误处理和重连机制

作者: [您的姓名]
创建时间: [创建日期]
版本: 1.2 (Bug修复版：状态同步)
"""

# 导入必要的库
import paho.mqtt.client as mqtt
import threading
import random
import time
import json
import mysql.connector
from mysql.connector import Error

# =============================================================================
# MQTT代理服务器配置
# =============================================================================
MQTT_BROKER_IP = 'localhost'
MQTT_BROKER_PORT = 1883
MQTT_TIMEOUT = 60

# =============================================================================
# MQTT主题配置
# =============================================================================
DATA_TOPIC = "stm32/data"
COMMAND_TOPIC_FORMAT = "stm32/command/{client_id}"

# =============================================================================
# MySQL数据库配置
# =============================================================================
MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'm2joy',
    'password': 'Liu041121@',
    'database': 'iot_data'
}


class MqttGateway:
    """
    MQTT物联网网关类 (V1.2 - Bug修复版)
    ======================================

    这个类实现了一个完整的MQTT网关功能，修复了状态同步的bug。
    核心改动：移除了内存状态管理，每次操作都从数据库读取最新状态，
    确保了Web端手动操作和网关自动控制之间的数据一致性。
    """

    def __init__(self, broker_ip, port, timeout):
        """
        初始化MQTT网关
        """
        self.broker_ip = broker_ip
        self.broker_port = port
        self.timeout = timeout
        self.connected = False
        self.client = None
        self.db_conn = self.setup_database()
        self.start_client()

    def setup_database(self):
        """
        设置并连接到MySQL数据库，如果表不存在则自动创建
        """
        try:
            conn = mysql.connector.connect(**MYSQL_CONFIG)
            if conn.is_connected():
                print("✅ 成功连接到MySQL数据库")
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS sensor_readings (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        client_id VARCHAR(255) NOT NULL,
                        temperature DECIMAL(5,2) NOT NULL,
                        humidity DECIMAL(4,1) NOT NULL,
                        light_intensity DECIMAL(7,1) NOT NULL,
                        fan_status TINYINT(1) DEFAULT 0,
                        light_status TINYINT(1) DEFAULT 0,
                        control_mode VARCHAR(10) DEFAULT 'auto',
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                conn.commit()
                cursor.close()
                print("📋 数据表 'sensor_readings' 已准备就绪")
                return conn
        except Error as e:
            print(f"❌ 数据库连接或操作失败: {e}")
            return None

    def start_client(self):
        """
        配置并启动Paho MQTT客户端
        """
        client_name = f"mqtt_gateway_{random.randint(1000, 9999)}"
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_name)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        try:
            print(f"🔄 正在连接MQTT代理服务器 {self.broker_ip}:{self.broker_port}")
            self.client.connect(self.broker_ip, self.broker_port, self.timeout)
            self.client.loop_start()
        except Exception as e:
            print(f"❌ 连接MQTT代理服务器失败: {e}")

    def on_connect(self, client, userdata, flags, rc, properties=None):
        """
        MQTT连接成功时的回调函数
        """
        if rc == 0:
            print("✅ 成功连接到MQTT代理服务器!")
            self.connected = True
            client.subscribe(DATA_TOPIC)
            print(f"📡 已订阅主题: {DATA_TOPIC}")
        else:
            print(f"❌ 连接失败，错误代码: {rc}")
            self.connected = False
    
    def get_latest_device_status(self, client_id):
        """
        从数据库获取指定设备的最新状态。
        这是修复bug的关键，确保我们总是在最新的状态上做决策。
        """
        if self.db_conn is None or not self.db_conn.is_connected():
            self.db_conn = self.setup_database()
        if self.db_conn is None:
            return None # 如果数据库无法连接，则返回None

        try:
            # 使用字典游标，方便按列名获取数据
            cursor = self.db_conn.cursor(dictionary=True)
            query = "SELECT fan_status, light_status, control_mode FROM sensor_readings WHERE client_id = %s ORDER BY timestamp DESC LIMIT 1"
            cursor.execute(query, (client_id,))
            status = cursor.fetchone()
            cursor.close()
            return status
        except Error as e:
            print(f"❌ 查询设备状态失败: {e}")
            return None


    def on_message(self, client, userdata, msg):
        """
        接收到MQTT消息时的核心处理函数 (已修复)
        """
        try:
            payload_str = msg.payload.decode('utf-8')
            print(f"📨 收到来自主题 '{msg.topic}' 的消息: {payload_str}")

            if not (payload_str.startswith('{') and payload_str.endswith('}')):
                print(f"❌ 格式错误: 消息没有被大括号包围 -> {payload_str}")
                return
            
            content = payload_str.strip('{}')
            parts = content.split(';')

            if len(parts) != 4:
                print(f"❌ 格式错误: 期望4个字段，但收到了 {len(parts)} 个 -> {payload_str}")
                return

            client_id, temp_str, hum_str, light_str = parts
            temperature = float(temp_str)
            humidity = float(hum_str)
            light_intensity = float(light_str)

            # --- 关键改动：从数据库获取设备最新状态 ---
            latest_status = self.get_latest_device_status(client_id)
            
            # 为新设备或查询失败的情况设置默认值
            db_fan_status = bool(latest_status['fan_status']) if latest_status else False
            db_light_status = bool(latest_status['light_status']) if latest_status else False
            db_control_mode = latest_status['control_mode'] if latest_status else 'auto'

            # --- 业务逻辑 ---
            # 如果是手动模式，网关只记录数据，不进行任何自动控制
            if db_control_mode == 'manual':
                print(f"🛑 设备 {client_id} 处于手动模式，跳过自动控制。")
                # 只保存新传感器数据，状态保持数据库中的不变
                self.save_to_db(client_id, temperature, humidity, light_intensity, db_fan_status, db_light_status, 'manual')
                return

            # 如果是自动模式，执行自动控制逻辑
            new_fan_state = db_fan_status
            new_light_state = db_light_status

            # 温度控制逻辑
            if temperature >= 30.0 and not db_fan_status:
                print(f"🚨 警报: 设备 {client_id} 的温度 ({temperature}°C) 过高，开启风扇!")
                self.publish_command(client_id, "open_fan")
                new_fan_state = True
            elif temperature <= 25.0 and db_fan_status:
                print(f"ℹ️  提示: 设备 {client_id} 的温度 ({temperature}°C) 正常，关闭风扇。")
                self.publish_command(client_id, "close_fan")
                new_fan_state = False

            # 光照控制逻辑
            if light_intensity < 50.0 and not db_light_status:
                print(f"🚨 警报: 设备 {client_id} 的光照 ({light_intensity}) 过低，开启补光灯!")
                self.publish_command(client_id, "open_light")
                new_light_state = True
            elif light_intensity >= 50.0 and db_light_status:
                print(f"ℹ️  提示: 设备 {client_id} 的光照 ({light_intensity}) 正常，关闭补光灯。")
                self.publish_command(client_id, "close_light")
                new_light_state = False
            
            # 保存包含最新传感器数据和最新状态的新记录
            self.save_to_db(client_id, temperature, humidity, light_intensity, new_fan_state, new_light_state, 'auto')

        except (ValueError, IndexError) as e:
            print(f"❌ 格式错误或数据转换失败: {e} -> {msg.payload.decode('utf-8')}")
        except Exception as e:
            print(f"❌ 处理消息时发生未知错误: {e}")

    def save_to_db(self, client_id, temperature, humidity, light_intensity, fan_status, light_status, mode):
        """
        将传感器数据和当前确定的状态保存到MySQL数据库。
        """
        if self.db_conn is None or not self.db_conn.is_connected():
            print("🔄 数据库连接已断开，尝试重新连接...")
            self.db_conn = self.setup_database()
            if self.db_conn is None:
                print("❌ 数据库重连失败，数据未保存")
                return

        try:
            cursor = self.db_conn.cursor()
            sql = """INSERT INTO sensor_readings 
                    (client_id, temperature, humidity, light_intensity, fan_status, light_status, control_mode) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s)"""
            
            # 将布尔值转换为整数 (1 或 0)
            val = (client_id, temperature, humidity, light_intensity, 1 if fan_status else 0, 1 if light_status else 0, mode)
            
            cursor.execute(sql, val)
            self.db_conn.commit()
            print(f"💾 数据已保存到数据库: 设备ID={client_id}, 模式={mode}, 风扇={'开' if fan_status else '关'}, 灯={'开' if light_status else '关'}")
            cursor.close()
        except Error as e:
            print(f"❌ 数据库插入操作失败: {e}")
            try:
                self.db_conn.close()
            except:
                pass
            self.db_conn = self.setup_database()

    def publish_command(self, client_id, command):
        """
        向指定设备发布控制指令
        """
        command_topic = COMMAND_TOPIC_FORMAT.format(client_id=client_id)
        command_payload = json.dumps({
            "command": command,
            "timestamp": time.time()
        })
        result = self.client.publish(command_topic, command_payload, qos=1)
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"📤 成功发送指令 '{command}' 到主题 '{command_topic}'")
        else:
            print(f"❌ 指令发送失败，错误代码: {result.rc}")


if __name__ == '__main__':
    print("🚀 启动MQTT物联网网关服务 (v1.2 - Bug修复版)...")
    print(f"📡 MQTT代理服务器: {MQTT_BROKER_IP}:{MQTT_BROKER_PORT}")
    print(f"🗄️  数据库: {MYSQL_CONFIG['host']}/{MYSQL_CONFIG['database']}")
    print("按 Ctrl+C 停止服务\n")
    
    gateway = MqttGateway(MQTT_BROKER_IP, MQTT_BROKER_PORT, MQTT_TIMEOUT)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 接收到停止信号，正在关闭网关服务...")
        if gateway.client:
            gateway.client.loop_stop()
            print("📡 MQTT客户端已停止")
        if gateway.db_conn and gateway.db_conn.is_connected():
            gateway.db_conn.close()
            print("🗄️  MySQL数据库连接已关闭")
        print("✅ 网关服务已安全关闭")
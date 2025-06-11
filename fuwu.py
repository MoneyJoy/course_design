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
版本: 1.0
"""

# 导入必要的库
import paho.mqtt.client as mqtt  # MQTT客户端库，用于与MQTT代理服务器通信
import threading                 # 线程库，用于多线程处理（虽然本例中未直接使用）
import random                    # 随机数库，用于生成唯一的客户端ID
import time                      # 时间库，用于时间戳和延时操作
import json                      # JSON库，用于解析和生成JSON格式的消息
import mysql.connector           # MySQL连接器，用于连接和操作MySQL数据库
from mysql.connector import Error  # MySQL错误处理类

# =============================================================================
# MQTT代理服务器配置
# =============================================================================
MQTT_BROKER_IP = 'localhost'     # MQTT代理服务器IP地址
                                 # 如果此脚本和EMQX在同一台服务器上，使用'localhost'
                                 # 如果在不同服务器上，请使用EMQX服务器的公网IP地址
MQTT_BROKER_PORT = 1883          # MQTT代理服务器端口，标准端口为1883
MQTT_TIMEOUT = 60                # MQTT连接超时时间（秒）

# =============================================================================
# MQTT主题配置
# =============================================================================
# 数据上行主题：STM32等设备发布传感器数据到此主题
DATA_TOPIC = "stm32/data"

# 指令下行主题格式：网关发布控制指令到此主题
# {client_id} 是占位符，会被实际的设备客户端ID替换
# 例如：stm32/command/device001
COMMAND_TOPIC_FORMAT = "stm32/command/{client_id}"

# =============================================================================
# MySQL数据库配置
# =============================================================================
# !!! 重要提示：请根据您的实际数据库设置修改以下配置信息 !!!
# MYSQL_CONFIG是一个字典(dict)类型，用于存储MySQL数据库的连接配置信息
MYSQL_CONFIG = {
    'host': 'localhost',         # MySQL服务器地址
    'user': 'm2joy',            # 数据库用户名
    'password': 'Liu041121@',    # 数据库密码
    'database': 'iot_data'       # 数据库名称
}


class MqttGateway:
    """
    MQTT物联网网关类
    ================

    这个类实现了一个完整的MQTT网关功能，包括：
    - 连接MQTT代理服务器（如EMQX）
    - 订阅设备数据主题，接收来自STM32等设备的传感器数据
    - 将接收到的数据存储到MySQL数据库中
    - 根据业务逻辑（如温度阈值）向设备下发控制指令
    - 提供完整的错误处理和重连机制

    主要功能流程：
    1. 初始化时自动连接数据库并创建必要的表
    2. 启动MQTT客户端并连接到代理服务器
    3. 订阅设备数据主题
    4. 接收并解析设备发送的JSON格式传感器数据
    5. 将数据存储到MySQL数据库
    6. 根据温度阈值判断是否需要发送控制指令
    """

    def __init__(self, broker_ip, port, timeout):
        """
        初始化MQTT网关

        参数:
            broker_ip (str): MQTT代理服务器的IP地址
            port (int): MQTT代理服务器的端口号（通常为1883）
            timeout (int): MQTT连接超时时间（秒）

        功能：
        1. 保存连接参数
        2. 初始化连接状态标志
        3. 设置数据库连接并创建必要的表
        4. 启动MQTT客户端连接
        """
        # 保存MQTT代理服务器连接参数
        self.broker_ip = broker_ip      # MQTT代理服务器IP地址
        self.broker_port = port         # MQTT代理服务器端口
        self.timeout = timeout          # 连接超时时间

        # 连接状态标志，用于跟踪MQTT连接状态
        self.connected = False

        # MQTT客户端对象，初始化为None
        self.client = None

        # 初始化数据库连接并创建必要的表
        # 这一步会自动连接到MySQL数据库并创建sensor_readings表（如果不存在）
        self.db_conn = self.setup_database()

        # 启动MQTT客户端并连接到代理服务器
        # 这一步会创建MQTT客户端、设置回调函数并开始连接
        self.start_client()

    def setup_database(self):
        """
        设置并连接到MySQL数据库，如果表不存在则自动创建

        返回值:
            mysql.connector.connection: 数据库连接对象，连接失败时返回None

        功能：
        1. 使用预定义的配置信息连接MySQL数据库
        2. 验证连接状态
        3. 创建sensor_readings表（如果不存在）
        4. 处理数据库连接和操作异常

        数据表结构：
        - id: 自增主键
        - client_id: 设备客户端ID（最大255字符）
        - temperature: 温度值（DECIMAL类型，5位数字，2位小数）
        - timestamp: 时间戳（自动设置为当前时间）
        """
        try:
            # 使用预定义的配置信息连接MySQL数据库
            # **MYSQL_CONFIG 会展开字典中的所有键值对作为参数
            conn = mysql.connector.connect(**MYSQL_CONFIG)

            # 验证数据库连接是否成功建立
            if conn.is_connected():
                print("✅ 成功连接到MySQL数据库")

                # 创建数据库游标，用于执行SQL语句
                cursor = conn.cursor()

                # 创建传感器数据表（如果不存在）
                # 使用IF NOT EXISTS确保表只在不存在时才创建
                # 注意：这里的SQL格式化是为了提高可读性
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS sensor_readings (
                        -- 自增主键，用于唯一标识每条记录
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        
                        -- 设备客户端ID，用于标识数据来源设备
                        -- VARCHAR(255)允许存储最大255个字符的设备标识符
                        client_id VARCHAR(255) NOT NULL,
                        
                        -- 温度传感器读数，使用DECIMAL(5,2)存储
                        -- 5表示总位数，2表示小数位数，范围：-999.99到999.99
                        temperature DECIMAL(5,2) NOT NULL,
                        
                        -- 数据记录时间戳，默认为当前时间
                        -- 使用DATETIME类型存储完整的日期和时间信息
                        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                    )
                ''')

                # 提交事务，确保表创建操作持久化
                conn.commit()

                # 关闭游标释放资源
                cursor.close()

                print("📋 数据表 'sensor_readings' 已准备就绪")
                return conn

        except Error as e:
            print(f"❌ 数据库连接或操作失败: {e}")
            return None

    def start_client(self):
        """
        配置并启动Paho MQTT客户端

        功能：
        1. 生成唯一的客户端ID
        2. 创建MQTT客户端实例
        3. 设置事件回调函数
        4. 连接到MQTT代理服务器
        5. 启动网络循环处理
        6. 处理连接异常

        注意：
        - 客户端ID必须在MQTT代理服务器中唯一
        - loop_start()会在后台线程中处理网络通信
        - 连接是异步的，实际连接结果在on_connect回调中处理
        """
        # 生成唯一的客户端ID，格式：mqtt_gateway_xxxx
        # 使用随机数确保每次运行时都有不同的客户端ID
        client_name = f"mqtt_gateway_{random.randint(1000, 9999)}"

        # 创建MQTT客户端实例
        # mqtt.CallbackAPIVersion.VERSION1 指定使用版本1的回调API
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_name)

        # 设置MQTT事件回调函数
        self.client.on_connect = self.on_connect    # 连接成功时的回调
        self.client.on_message = self.on_message    # 接收消息时的回调

        try:
            # 连接到MQTT代理服务器
            # 参数：IP地址、端口、保持连接超时时间
            self.client.connect(self.broker_ip, self.broker_port, self.timeout)

            # 启动网络循环，在后台线程中处理网络通信
            # 这是非阻塞的，允许主线程继续执行其他任务
            self.client.loop_start()

            print(f"🔄 正在连接MQTT代理服务器 {self.broker_ip}:{self.broker_port}")

        except Exception as e:
            print(f"❌ 连接MQTT代理服务器失败: {e}")

    def on_connect(self, client, userdata, flags, rc, properties=None):
        """
        MQTT连接成功时的回调函数

        参数:
            client: MQTT客户端实例
            userdata: 用户数据（本例中未使用，但保留以符合回调函数签名）
            flags: 连接标志（本例中未使用，但保留以符合回调函数签名）
            rc (int): 连接结果代码
                     0: 连接成功
                     1: 协议版本错误
                     2: 客户端ID无效
                     3: 服务器不可用
                     4: 用户名或密码错误
                     5: 未授权

        功能：
        1. 检查连接结果代码
        2. 设置连接状态标志
        3. 订阅设备数据主题
        4. 输出连接状态信息

        注意：
        - 只有在连接成功（rc=0）时才会订阅主题
        - 订阅使用默认的QoS级别0（最多一次传递）
        """
        if rc == 0:
            print("✅ 成功连接到MQTT代理服务器!")

            # 设置连接状态标志为True
            self.connected = True

            # 订阅设备数据主题
            # QoS=0表示"最多一次"传递，消息可能丢失但不会重复
            client.subscribe(DATA_TOPIC)
            print(f"📡 已订阅主题: {DATA_TOPIC}")
        else:
            print(f"❌ 连接失败，错误代码: {rc}")
            self.connected = False

    def on_message(self, client, userdata, msg):
        """
        接收到MQTT消息时的核心处理函数

        参数:
            client: MQTT客户端实例（本例中未使用，但保留以符合回调函数签名）
            userdata: 用户数据（本例中未使用，但保留以符合回调函数签名）
            msg: 接收到的消息对象，包含以下属性：
                 - topic: 消息主题
                 - payload: 消息载荷（字节格式）
                 - qos: 消息的QoS级别
                 - retain: 是否为保留消息

        功能：
        1. 解码消息载荷（从字节转换为UTF-8字符串）
        2. 解析JSON格式的传感器数据
        3. 验证数据完整性（检查必需字段）
        4. 将数据保存到MySQL数据库
        5. 根据业务逻辑判断是否需要发送控制指令
        6. 处理各种异常情况

        预期的消息格式（JSON）：
        {
            "client_id": "设备客户端ID",
            "temperature": 温度值（数字）
        }

        业务逻辑：
        - 当温度超过30°C时，向设备发送"open_fan"指令
        """
        try:
            # 将消息载荷从字节格式解码为UTF-8字符串
            payload_str = msg.payload.decode('utf-8')
            print(f"📨 收到来自主题 '{msg.topic}' 的消息: {payload_str}")

            # 解析JSON格式的消息数据
            data = json.loads(payload_str)

            # 提取关键数据字段
            client_id = data.get('client_id')      # 设备客户端ID
            temperature = data.get('temperature')  # 温度值

            # 验证数据完整性 - 检查必需字段是否存在
            if client_id is None or temperature is None:
                print("⚠️  警告: 接收到的数据缺少 'client_id' 或 'temperature' 字段")
                return

            # 将传感器数据保存到数据库
            self.save_to_db(client_id, temperature)

            # 业务逻辑：温度阈值检查
            # 如果温度超过30°C，向设备发送开启风扇的指令
            if float(temperature) > 30.0:
                print(f"🚨 警报: 设备 {client_id} 的温度 ({temperature}°C) 超过阈值!")
                self.publish_command(client_id, "open_fan")

        except json.JSONDecodeError:
            # JSON解析失败的异常处理
            print(f"❌ JSON解析失败，无效的载荷格式: {msg.payload}")
        except Exception as e:
            # 其他异常的通用处理
            print(f"❌ 处理消息时发生错误: {e}")

    def save_to_db(self, client_id, temperature):
        """
        将传感器数据保存到MySQL数据库

        参数:
            client_id (str): 设备客户端ID
            temperature (float): 温度值

        功能：
        1. 检查数据库连接状态
        2. 如果连接断开，尝试重新连接
        3. 执行SQL插入操作
        4. 提交事务确保数据持久化
        5. 处理数据库操作异常
        6. 在发生错误时尝试重新连接数据库

        数据库操作说明：
        - 使用参数化查询防止SQL注入攻击
        - timestamp字段会自动设置为当前时间（数据库默认值）
        - 使用事务确保数据一致性
        """
        # 检查数据库连接状态
        if self.db_conn is None or not self.db_conn.is_connected():
            print("🔄 数据库连接已断开，尝试重新连接...")

            # 尝试重新建立数据库连接
            self.db_conn = self.setup_database()

            # 如果重连失败，放弃保存操作
            if self.db_conn is None:
                print("❌ 数据库重连失败，数据未保存")
                return

        try:
            # 创建数据库游标
            cursor = self.db_conn.cursor()

            # 定义SQL插入语句
            # 注意：MySQL-connector使用%s作为参数占位符（不是%d或%f）
            # timestamp字段使用数据库默认值（CURRENT_TIMESTAMP）
            sql = "INSERT INTO sensor_readings (client_id, temperature) VALUES (%s, %s)"
            val = (client_id, temperature)

            # 执行SQL语句
            cursor.execute(sql, val)

            # 提交事务，确保数据持久化到数据库
            self.db_conn.commit()

            print(f"💾 数据已保存到数据库: 设备ID={client_id}, 温度={temperature}°C")

            # 关闭游标释放资源
            cursor.close()

        except Error as e:
            print(f"❌ 数据库插入操作失败: {e}")

            # 发生错误时关闭当前连接并尝试重新连接
            # 这有助于处理数据库连接超时等问题
            try:
                self.db_conn.close()
            except:
                pass  # 忽略关闭连接时的异常

            # 尝试重新建立数据库连接，为下次操作做准备
            self.db_conn = self.setup_database()

    def publish_command(self, client_id, command):
        """
        向指定设备发布控制指令

        参数:
            client_id (str): 目标设备的客户端ID
            command (str): 要发送的控制指令（如"open_fan", "close_fan"等）

        功能：
        1. 根据设备ID构造专用的指令主题
        2. 创建包含指令和时间戳的JSON载荷
        3. 使用QoS=1级别发布指令消息
        4. 检查发布结果并输出状态信息

        主题格式：
        - 使用COMMAND_TOPIC_FORMAT模板
        - 例如：stm32/command/device001

        消息格式（JSON）：
        {
            "command": "指令名称",
            "timestamp": Unix时间戳
        }

        QoS级别说明：
        - QoS=1: "至少一次"传递，确保消息到达但可能重复
        - 适用于重要的控制指令，确保设备能收到
        """
        # 根据设备客户端ID构造指令主题
        # 例如：stm32/command/device001
        command_topic = COMMAND_TOPIC_FORMAT.format(client_id=client_id)

        # 创建JSON格式的指令载荷
        command_payload = json.dumps({
            "command": command,                    # 控制指令
            "timestamp": time.time()              # Unix时间戳，用于指令去重和超时检查
        })

        # 发布指令消息到指定主题
        # qos=1 确保消息至少传递一次，提高指令传递的可靠性
        result = self.client.publish(command_topic, command_payload, qos=1)

        # 检查发布结果
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"📤 成功发送指令 '{command}' 到主题 '{command_topic}'")
        else:
            print(f"❌ 指令发送失败，错误代码: {result.rc}")


# =============================================================================
# 主程序入口
# =============================================================================
if __name__ == '__main__':
    """
    主程序入口点

    功能：
    1. 创建MQTT网关实例并启动服务
    2. 保持程序运行，等待MQTT消息
    3. 处理用户中断信号（Ctrl+C）
    4. 优雅地关闭所有连接和资源

    运行流程：
    1. 使用预定义的配置参数创建网关实例
    2. 进入无限循环，保持程序运行
    3. 当用户按Ctrl+C时，捕获KeyboardInterrupt异常
    4. 执行清理操作：停止MQTT循环、关闭数据库连接
    5. 输出关闭状态信息并退出程序
    """
    print("🚀 启动MQTT物联网网关服务...")
    print(f"📡 MQTT代理服务器: {MQTT_BROKER_IP}:{MQTT_BROKER_PORT}")
    print(f"🗄️  数据库: {MYSQL_CONFIG['host']}/{MYSQL_CONFIG['database']}")
    print("按 Ctrl+C 停止服务\n")

    # 创建MQTT网关实例
    # 这会自动连接数据库、创建表、连接MQTT代理服务器并订阅主题
    gateway = MqttGateway(MQTT_BROKER_IP, MQTT_BROKER_PORT, MQTT_TIMEOUT)

    try:
        # 主循环：保持程序运行
        # 实际的MQTT消息处理在后台线程中进行
        while True:
            time.sleep(1)  # 每秒检查一次，避免CPU占用过高

    except KeyboardInterrupt:
        # 捕获用户中断信号（Ctrl+C），执行优雅关闭
        print("\n🛑 接收到停止信号，正在关闭网关服务...")

        # 停止MQTT客户端的网络循环
        if gateway.client:
            gateway.client.loop_stop()
            print("📡 MQTT客户端已停止")

        # 关闭数据库连接
        if gateway.db_conn and gateway.db_conn.is_connected():
            gateway.db_conn.close()
            print("🗄️  MySQL数据库连接已关闭")

        print("✅ 网关服务已安全关闭")

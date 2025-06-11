## 1. 安装MySQL服务器
---
```bash
sudo apt update
sudo apt install mysql-server
```
## 2. 运行安全配置脚本
---
```bash
sudo mysql_secure_installation
```
## 3. 创建数据库和专用用户
---
```bash
sudo mysql

-- 创建一个名为 'iot_data' 的新数据库
CREATE DATABASE iot_data;

-- 创建一个新用户 'gateway_user'，并设置密码（请将 'YourStrongPassword' 替换为您自己的强密码）
CREATE USER 'gateway_user'@'localhost' IDENTIFIED BY 'YourStrongPassword';

-- 将 'iot_data' 数据库的所有权限授予这个新用户
GRANT ALL PRIVILEGES ON iot_data.* TO 'gateway_user'@'localhost';

-- 刷新权限使更改生效
FLUSH PRIVILEGES;

-- 退出MySQL
EXIT;
```
## 4. 项目代码
---
**项目地址**
`https://github.com/MoneyJoy/course_design`

requirements.txt文件管理依赖:
```bash
flask==2.0.1
mysql-connector-python==8.0.26
gunicorn==20.1.0 
werkzeug==2.3.0
```

创建一个完整的Web应用来显示传感器数据。以下是使用说明和将应用映射到公网的步骤：

1. **安装依赖**
```bash
pip install -r requirements.txt
```

2. **本地测试运行**
```bash
python web_display.py
```
运行后，可以通过 `http://localhost:5000` 访问网页。

3. **将应用映射到公网的步骤**：

a) **使用Nginx作为反向代理**：
```bash
# 安装Nginx
sudo apt-get update
sudo apt-get install nginx

# 创建Nginx配置文件
sudo nano /etc/nginx/sites-available/iot_display
```

在配置文件中添加以下内容：
```nginx
server {
    listen 80;
    server_name your_domain_or_ip;  # 替换为您的域名或服务器IP

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

启用配置：
```bash
sudo ln -s /etc/nginx/sites-available/iot_display /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx
```

b) **使用Gunicorn运行Flask应用**：
```bash
# 创建systemd服务文件
sudo nano /etc/systemd/system/iot_display.service
```

添加以下内容：
```bash
[Unit]
Description=IoT Display Web Application
After=network.target

[Service]
User=root
WorkingDirectory=/root/my_project
Environment="PATH=/root/my_project/venv/bin"
ExecStart=/root/my_project/venv/bin/gunicorn --workers 3 --bind 127.0.0.1:5000 web_display:app

[Install]
WantedBy=multi-user.target
```

启动服务：
```bash
# 重新加载systemd配置
sudo systemctl daemon-reload

# 启动服务
sudo systemctl start iot_display

# 设置开机自启
sudo systemctl enable iot_display

# 查看服务状态
sudo systemctl status iot_display
```

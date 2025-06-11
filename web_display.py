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

    try:
        cursor = conn.cursor(dictionary=True)
        # Get last 100 records, ordered by timestamp
        cursor.execute("""
            SELECT client_id, temperature, timestamp 
            FROM sensor_readings 
            ORDER BY timestamp DESC 
            LIMIT 100
        """)
        readings = cursor.fetchall()
        
        # Format timestamp
        for reading in readings:
            reading['timestamp'] = reading['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
        
        return render_template('index.html', readings=readings)
    except mysql.connector.Error as err:
        print(f"Query error: {err}")
        return "Database query failed", 500
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    # Create templates directory if not exists
    if not os.path.exists('templates'):
        os.makedirs('templates')
    
    # Create HTML template
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
        .temperature-high { color: #dc3545; }
        .temperature-low { color: #0d6efd; }
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
                        <th>Temperature (°„C)</th>
                        <th>Time</th>
                    </tr>
                </thead>
                <tbody>
                    {% for reading in readings %}
                    <tr>
                        <td>{{ reading.client_id }}</td>
                        <td class="{% if reading.temperature > 30 %}temperature-high{% elif reading.temperature < 25 %}temperature-low{% endif %}">
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
        // Auto refresh every 30 seconds
        setTimeout(function() {
            window.location.reload();
        }, 30000);
    </script>
</body>
</html>
        ''')
    
    # Start Flask application
    app.run(host='0.0.0.0', port=5000) 
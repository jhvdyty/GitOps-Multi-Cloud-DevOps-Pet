from flask import Flask, jsonify, request
import requests
import time
from collections import defaultdict, deque

app = Flask(__name__)

# rate limiter в памяти
rate_limit_storage = defaultdict(lambda: deque())
RATE_LIMIT = 10  # запросов в минуту
WINDOW_SIZE = 60  # секунд

def is_rate_limited(client_ip):
    now = time.time()
    client_requests = rate_limit_storage[client_ip]
    
    # del старые запросы
    while client_requests and client_requests[0] < now - WINDOW_SIZE:
        client_requests.popleft()
    
    # лимит
    if len(client_requests) >= RATE_LIMIT:
        return True
    
    # текущий запрос
    client_requests.append(now)
    return False

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy", "service": "api-gateway"})

@app.route('/api/weather/<city>', methods=['GET'])
def get_weather(city):
    client_ip = request.remote_addr
    
    # чекаем rate limit
    if is_rate_limited(client_ip):
        return jsonify({"error": "Rate limit exceeded. Try again later."}), 429
    
    try:
        # Перенаправляем запрос к data-processor
        response = requests.get(f'http://data-processor:5002/process/{city}', timeout=15)
        
        if response.status_code == 200:
            return jsonify(response.json())
        else:
            return jsonify({"error": "Weather service unavailable"}), 503
    
    except Exception as e:
        return jsonify({"error": f"Weather service error: {str(e)}"}), 500

@app.route('/api/analytics/<city>', methods=['GET'])
def get_analytics(city):
    client_ip = request.remote_addr
    
    if is_rate_limited(client_ip):
        return jsonify({"error": "Rate limit exceeded. Try again later."}), 429
    
    try:
        # Перенаправляем запрос к analytics service (правильный маршрут)
        response = requests.get(f'http://analytics:5003/analyze/{city}', timeout=15)
        
        if response.status_code == 200:
            return jsonify(response.json())
        else:
            return jsonify({"error": "Analytics service unavailable"}), 503
    
    except Exception as e:
        return jsonify({"error": f"Analytics service error: {str(e)}"}), 500

@app.route('/api/history/<city>', methods=['GET'])
def get_history(city):
    client_ip = request.remote_addr
    
    if is_rate_limited(client_ip):
        return jsonify({"error": "Rate limit exceeded. Try again later."}), 429
    
    try:
        # Перенаправляем запрос к analytics service
        response = requests.get(f'http://analytics:5003/history/{city}', timeout=15)
        
        if response.status_code == 200:
            return jsonify(response.json())
        else:
            return jsonify({"error": "History service unavailable"}), 503
    
    except Exception as e:
        return jsonify({"error": f"History service error: {str(e)}"}), 500

@app.route('/api/status', methods=['GET'])
def system_status():
    services = {
        'weather-aggregator': 'http://weather-aggregator:5001/health',
        'data-processor': 'http://data-processor:5002/health',
        'analytics': 'http://analytics:5003/health'
    }
    
    status = {}
    for service, url in services.items():
        try:
            response = requests.get(url, timeout=5)
            status[service] = "healthy" if response.status_code == 200 else "unhealthy"
        except:
            status[service] = "unavailable"
    
    return jsonify(status)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
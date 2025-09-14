from flask import Flask, jsonify, request
import grpc
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'generated'))

import data_processor_pb2
import data_processor_pb2_grpc
import analytics_pb2
import analytics_pb2_grpc
import time
from collections import defaultdict, deque

app = Flask(__name__)

# Rate limiting
rate_limit_storage = defaultdict(lambda: deque())
RATE_LIMIT = 10
WINDOW_SIZE = 60

def is_rate_limited(client_ip):
    now = time.time()
    client_requests = rate_limit_storage[client_ip]
    
    while client_requests and client_requests[0] < now - WINDOW_SIZE:
        client_requests.popleft()
    
    if len(client_requests) >= RATE_LIMIT:
        return True
    
    client_requests.append(now)
    return False

class GrpcClients:
    def __init__(self):
        self.data_processor_channel = grpc.insecure_channel('data-processor:50052')
        self.data_processor_client = data_processor_pb2_grpc.DataProcessorServiceStub(
            self.data_processor_channel
        )
        
        self.analytics_channel = grpc.insecure_channel('analytics:50053')
        self.analytics_client = analytics_pb2_grpc.AnalyticsServiceStub(
            self.analytics_channel
        )

grpc_clients = GrpcClients()

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy", "service": "api-gateway"})

@app.route('/api/weather/<city>', methods=['GET'])
def get_weather(city):
    client_ip = request.remote_addr
    
    if is_rate_limited(client_ip):
        return jsonify({"error": "Rate limit exceeded. Try again later."}), 429
    
    try:
        # Вызываем data-processor через gRPC
        process_request = data_processor_pb2.ProcessRequest(city=city)
        response = grpc_clients.data_processor_client.ProcessWeatherData(process_request)
        
        # Конвертируем gRPC ответ в JSON
        result = {
            "city": response.city,
            "processed_at": response.processed_at,
            "weather_summary": {
                "openweather": {
                    "temperature": response.weather_summary.openweather.temperature,
                    "humidity": response.weather_summary.openweather.humidity,
                    "pressure": response.weather_summary.openweather.pressure,
                    "description": response.weather_summary.openweather.description,
                    "wind_speed": response.weather_summary.openweather.wind_speed
                } if response.weather_summary.HasField('openweather') else None,
                "weatherapi": {
                    "temperature": response.weather_summary.weatherapi.temperature,
                    "humidity": response.weather_summary.weatherapi.humidity,
                    "pressure": response.weather_summary.weatherapi.pressure,
                    "description": response.weather_summary.weatherapi.description,
                    "wind_speed": response.weather_summary.weatherapi.wind_speed
                } if response.weather_summary.HasField('weatherapi') else None
            },
            "averages": {
                "temperature": response.averages.temperature,
                "humidity": response.averages.humidity
            },
            "data_sources": {
                "openweather": response.data_sources.openweather,
                "weatherapi": response.data_sources.weatherapi
            }
        }
        
        return jsonify(result)
        
    except grpc.RpcError as e:
        return jsonify({"error": f"gRPC error: {str(e)}"}), 500
    except Exception as e:
        return jsonify({"error": f"Weather service error: {str(e)}"}), 500

@app.route('/api/analytics/<city>', methods=['GET'])
def get_analytics(city):
    client_ip = request.remote_addr
    
    if is_rate_limited(client_ip):
        return jsonify({"error": "Rate limit exceeded. Try again later."}), 429
    
    try:
        # Вызываем analytics через gRPC
        analyze_request = analytics_pb2.AnalyzeRequest(city=city)
        response = grpc_clients.analytics_client.AnalyzeWeather(analyze_request)
        
        result = {
            "city": response.city,
            "analysis_time": response.analysis_time,
            "total_requests": response.total_requests,
            "current_weather": {
                "temperature": response.current_weather.temperature,
                "humidity": response.current_weather.humidity
            },
            "insights": list(response.insights),
            "temperature_trend": {
                "current": response.temperature_trend.current,
                "recent_average": response.temperature_trend.recent_average,
                "data_points": response.temperature_trend.data_points
            } if response.HasField('temperature_trend') else None
        }
        
        return jsonify(result)
        
    except grpc.RpcError as e:
        return jsonify({"error": f"gRPC Analytics error: {str(e)}"}), 500

@app.route('/api/history/<city>', methods=['GET'])
def get_history(city):
    client_ip = request.remote_addr
    
    if is_rate_limited(client_ip):
        return jsonify({"error": "Rate limit exceeded. Try again later."}), 429
    
    try:
        history_request = analytics_pb2.HistoryRequest(city=city)
        response = grpc_clients.analytics_client.GetHistory(history_request)
        
        return jsonify({
            "city": response.city,
            "total_records": response.total_records,
            "history": list(response.history)
        })
        
    except grpc.RpcError as e:
        return jsonify({"error": f"gRPC History error: {str(e)}"}), 500

@app.route('/api/status', methods=['GET'])
def system_status():
    status = {}
    
    # Проверяем data-processor
    try:
        health_request = data_processor_pb2.Empty()
        response = grpc_clients.data_processor_client.HealthCheck(health_request)
        status['data-processor'] = response.status
    except:
        status['data-processor'] = "unavailable"
    
    # Проверяем analytics
    try:
        health_request = analytics_pb2.Empty()
        response = grpc_clients.analytics_client.HealthCheck(health_request)
        status['analytics'] = response.status
    except:
        status['analytics'] = "unavailable"
    
    return jsonify(status)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
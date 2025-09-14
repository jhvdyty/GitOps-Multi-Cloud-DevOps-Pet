from flask import Flask, jsonify
import threading
from grpc_server import serve

app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy", "service": "weather-aggregator"})

# Запускаем gRPC сервер в отдельном потоке
def start_grpc_server():
    serve()

if __name__ == '__main__':
    # Запускаем gRPC сервер в фоне
    grpc_thread = threading.Thread(target=start_grpc_server, daemon=True)
    grpc_thread.start()
    
    # Запускаем Flask для health check
    app.run(host='0.0.0.0', port=5001, debug=False)
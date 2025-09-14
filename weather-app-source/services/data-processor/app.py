from flask import Flask, jsonify
import threading
from grpc_server import serve

app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy", "service": "data-processor"})

if __name__ == '__main__':
    # gRPC сервер в фоне
    grpc_thread = threading.Thread(target=serve, daemon=True)
    grpc_thread.start()
    
    # Flask для health check
    app.run(host='0.0.0.0', port=5002, debug=False)
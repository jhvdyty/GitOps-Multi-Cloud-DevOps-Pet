from flask import Flask, jsonify
import requests
from datetime import datetime, timedelta

app = Flask(__name__)

# хранилище в памяти для истории запросов
weather_history = []

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy", "service": "analytics"})

@app.route('/analyze/<city>', methods=['GET'])
def analyze_weather(city):
    try:
        # текущие обработанные данные
        response = requests.get(f'http://data-processor:5002/process/{city}', timeout=10)
        if response.status_code != 200:
            return jsonify({"error": "Failed to fetch weather data"}), 500
        
        current_data = response.json()
        
        # история
        weather_history.append({
            "city": city,
            "timestamp": datetime.now().isoformat(),
            "data": current_data
        })
        
        # размер истории (последние 100 записей)
        if len(weather_history) > 100:
            weather_history.pop(0)
        
        # данные для этого города
        city_history = [item for item in weather_history if item['city'].lower() == city.lower()]
        
        analytics = {
            "city": city,
            "analysis_time": datetime.now().isoformat(),
            "total_requests": len(city_history),
            "current_weather": current_data.get('averages', {}),
            "insights": []
        }
        
        # простая аналитика
        if len(city_history) > 1:
            recent_temps = []
            for record in city_history[-5:]:  # последние 5
                temp = record.get('data', {}).get('averages', {}).get('temperature')
                if temp is not None:
                    recent_temps.append(temp)
            
            if recent_temps:
                avg_temp = sum(recent_temps) / len(recent_temps)
                current_temp = current_data.get('averages', {}).get('temperature')
                
                if current_temp is not None:
                    if current_temp > avg_temp + 2:
                        analytics['insights'].append("Температура выше среднего значения")
                    elif current_temp < avg_temp - 2:
                        analytics['insights'].append("Температура ниже среднего значения")
                    else:
                        analytics['insights'].append("Температура в пределах нормы")
                
                analytics['temperature_trend'] = {
                    "current": current_temp,
                    "recent_average": round(avg_temp, 2),
                    "data_points": len(recent_temps)
                }
        
        return jsonify(analytics)
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/history/<city>', methods=['GET'])
def get_weather_history(city):
    city_history = [item for item in weather_history if item['city'].lower() == city.lower()]
    return jsonify({
        "city": city,
        "total_records": len(city_history),
        "history": city_history[-10:]  # последние 10
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003, debug=True)
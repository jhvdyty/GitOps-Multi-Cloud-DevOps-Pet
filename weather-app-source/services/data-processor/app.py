from flask import Flask, jsonify, request
import requests
from datetime import datetime

app = Flask(__name__)

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy", "service": "data-processor"})

@app.route('/process/<city>', methods=['GET'])
def process_weather_data(city):
    try:
        # weather-aggregator
        response = requests.get(f'http://weather-aggregator:5001/weather/{city}', timeout=15)
        if response.status_code != 200:
            return jsonify({"error": f"Weather aggregator returned status {response.status_code}"}), 500
        
        raw_data = response.json()
        
        # есть ли данные хотя бы из одного источника
        if not raw_data.get('openweather') and not raw_data.get('weatherapi'):
            return jsonify({"error": "No weather data available from any source"}), 500
        
        # нормализуем данные
        processed_data = {
            "city": city,
            "processed_at": datetime.now().isoformat(),
            "weather_summary": {},
            "data_sources": raw_data.get('status', {})
        }
        
        # OpenWeatherMap
        if raw_data.get('openweather') and isinstance(raw_data['openweather'], dict) and 'main' in raw_data['openweather']:
            ow_data = raw_data['openweather']
            processed_data['weather_summary']['openweather'] = {
                "temperature": ow_data['main'].get('temp'),
                "humidity": ow_data['main'].get('humidity'),
                "pressure": ow_data['main'].get('pressure'),
                "description": ow_data['weather'][0].get('description') if ow_data.get('weather') else None,
                "wind_speed": ow_data.get('wind', {}).get('speed')
            }
        
        # WeatherAPI
        if raw_data.get('weatherapi') and isinstance(raw_data['weatherapi'], dict) and 'current' in raw_data['weatherapi']:
            wa_data = raw_data['weatherapi']['current']
            processed_data['weather_summary']['weatherapi'] = {
                "temperature": wa_data.get('temp_c'),
                "humidity": wa_data.get('humidity'),
                "pressure": wa_data.get('pressure_mb'),
                "description": wa_data.get('condition', {}).get('text'),
                "wind_speed": wa_data.get('wind_kph')
            }
        
        # средние значения
        temps = []
        humidities = []
        
        for source in processed_data['weather_summary'].values():
            if isinstance(source, dict):
                if source.get('temperature') is not None:
                    temps.append(source['temperature'])
                if source.get('humidity') is not None:
                    humidities.append(source['humidity'])
        
        processed_data['averages'] = {
            "temperature": round(sum(temps) / len(temps), 2) if temps else None,
            "humidity": round(sum(humidities) / len(humidities), 2) if humidities else None
        }
        
        return jsonify(processed_data)
    
    except requests.exceptions.RequestException as e:
        return jsonify({"error": f"Failed to connect to weather aggregator: {str(e)}"}), 500
    except Exception as e:
        return jsonify({"error": f"Data processing error: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002, debug=True)
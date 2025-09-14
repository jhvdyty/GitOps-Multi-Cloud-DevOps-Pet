import grpc
from concurrent import futures
import sys 
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'generated'))

import weather_service_pb2
import weather_service_pb2_grpc
import requests
from datetime import datetime

class WeatherService(weather_service_pb2_grpc.WeatherServiceServicer):
    def __init__(self):
        self.openweather_key = os.getenv('OPENWEATHER_API_KEY', 'your_key')
        self.weatherapi_key = os.getenv('WEATHERAPI_KEY', 'your_key')

    def GetWeather(self, request, context):
        city = request.city
        
        # OpenWeatherMap
        openweather_data = self._get_openweather_data(city)
        
        # WeatherAPI
        weatherapi_data = self._get_weatherapi_data(city)
        
        # создаем статус
        status = weather_service_pb2.SourceStatus(
            openweather="success" if openweather_data.available else "failed",
            weatherapi="success" if weatherapi_data.available else "failed"
        )
         
        return weather_service_pb2.WeatherResponse(
            city=city,
            openweather=openweather_data,
            weatherapi=weatherapi_data,
            timestamp=datetime.now().isoformat(),
            status=status
        )
    
    def HealthCheck(self, request, context):
        return weather_service_pb2.HealthResponse(
            status="healthy",
            service="weather-aggregator"
        )
    
    def _get_openweather_data(self, city):
        try:
            url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={self.openweather_key}&units=metric"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                return weather_service_pb2.WeatherData(
                    temperature=data['main'].get('temp', 0),
                    humidity=data['main'].get('humidity', 0),
                    pressure=data['main'].get('pressure', 0),
                    description=data['weather'][0].get('description', '') if data.get('weather') else '',
                    wind_speed=data.get('wind', {}).get('speed', 0),
                    available=True
                )
        except Exception as e:
            print(f"OpenWeather API error: {e}")
        
        return weather_service_pb2.WeatherData(available=False)
    
    def _get_weatherapi_data(self, city):
        try:
            url = f"http://api.weatherapi.com/v1/current.json?key={self.weatherapi_key}&q={city}"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                current = data.get('current', {})
                return weather_service_pb2.WeatherData(
                    temperature=current.get('temp_c', 0),
                    humidity=current.get('humidity', 0),
                    pressure=current.get('pressure_mb', 0),
                    description=current.get('condition', {}).get('text', ''),
                    wind_speed=current.get('wind_kph', 0),
                    available=True
                )
        except Exception as e:
            print(f"WeatherAPI error: {e}")
        
        return weather_service_pb2.WeatherData(available=False)
    
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    weather_service_pb2_grpc.add_WeatherServiceServicer_to_server(
        WeatherService(), server
    )
    server.add_insecure_port('[::]:50051')
    
    print("gRPC Weather Service порт: 50051...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
import grpc
from concurrent import futures
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'generated'))

import data_processor_pb2
import data_processor_pb2_grpc
import weather_service_pb2
import weather_service_pb2_grpc
from datetime import datetime

class DataProcessorService(data_processor_pb2_grpc.DataProcessorServiceServicer):
    def __init__(self):
        # клиент для weather-aggregator
        self.weather_channel = grpc.insecure_channel('weather-aggregator:50051')
        self.weather_client = weather_service_pb2_grpc.WeatherServiceStub(self.weather_channel)
    
    def ProcessWeatherData(self, request, context):
        city = request.city
        
        try:
            # Получаем данные через gRPC от weather-aggregator
            weather_request = weather_service_pb2.WeatherRequest(city=city)
            weather_response = self.weather_client.GetWeather(weather_request)
            
            # Обрабатываем данные
            weather_summary = data_processor_pb2.WeatherSummary()
            
            if weather_response.openweather.available:
                weather_summary.openweather.CopyFrom(self._convert_weather_data(weather_response.openweather))
            
            if weather_response.weatherapi.available:
                weather_summary.weatherapi.CopyFrom(self._convert_weather_data(weather_response.weatherapi))
            
            # Вычисляем средние значения
            temps = []
            humidities = []
            
            if weather_response.openweather.available:
                temps.append(weather_response.openweather.temperature)
                humidities.append(weather_response.openweather.humidity)
            
            if weather_response.weatherapi.available:
                temps.append(weather_response.weatherapi.temperature)
                humidities.append(weather_response.weatherapi.humidity)
            
            averages = data_processor_pb2.Averages(
                temperature=sum(temps) / len(temps) if temps else 0,
                humidity=sum(humidities) / len(humidities) if humidities else 0
            )
            
            data_sources = data_processor_pb2.DataSources(
                openweather=weather_response.status.openweather,
                weatherapi=weather_response.status.weatherapi
            )
            
            return data_processor_pb2.ProcessResponse(
                city=city,
                processed_at=datetime.now().isoformat(),
                weather_summary=weather_summary,
                averages=averages,
                data_sources=data_sources
            )
            
        except grpc.RpcError as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Failed to get weather data: {str(e)}")
            return data_processor_pb2.ProcessResponse()
        
    def HealthCheck(self, request, context):
        return data_processor_pb2.HealthResponse(
            status="healthy",
            service="data-processor"
        )
        
    def _convert_weather_data(self, weather_data):
        return data_processor_pb2.SourceData(
            temperature=weather_data.temperature,
            humidity=weather_data.humidity,
            pressure=weather_data.pressure,
            description=weather_data.description,
            wind_speed=weather_data.wind_speed
        )
    
def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    data_processor_pb2_grpc.add_DataProcessorServiceServicer_to_server(
        DataProcessorService(), server
    )
    server.add_insecure_port('[::]:50052')
    
    print("gRPC Data Processor Service запущен на порту 50052...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
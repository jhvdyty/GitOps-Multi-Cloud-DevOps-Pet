import grpc
from concurrent import futures
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'generated'))

import analytics_pb2
import analytics_pb2_grpc
import data_processor_pb2
import data_processor_pb2_grpc
from datetime import datetime
import json

# хранилище для истории запросов
weather_history = []

class AnalyticsService(analytics_pb2_grpc.AnalyticsServiceServicer):
    def __init__(self):
        # клиент для data-processor
        self.data_processor_channel = grpc.insecure_channel('data-processor:50052')
        self.data_processor_client = data_processor_pb2_grpc.DataProcessorServiceStub(self.data_processor_channel)
    
    def AnalyzeWeather(self, request, context):
        city = request.city
        
        try:
            # получаем текущие обработанные данные через gRPC
            process_request = data_processor_pb2.ProcessRequest(city=city)
            current_data = self.data_processor_client.ProcessWeatherData(process_request)

            history_record = {
                "city": city,
                "timestamp": datetime.now().isoformat(),
                "data": {
                    "averages": {
                        "temperature": current_data.averages.temperature,
                        "humidity": current_data.averages.humidity
                    }
                }
            }
            
            weather_history.append(history_record)
            
            # ограничиваем размер истории
            if len(weather_history) > 100:
                weather_history.pop(0)
            
            city_history = [item for item in weather_history if item['city'].lower() == city.lower()]
            
            current_weather = analytics_pb2.CurrentWeather(
                temperature=current_data.averages.temperature,
                humidity=current_data.averages.humidity
            )
            
            insights = []
            temperature_trend = None
            
            if len(city_history) > 1:
                recent_temps = []
                for record in city_history[-5:]:  # последние 5 записей
                    temp = record.get('data', {}).get('averages', {}).get('temperature')
                    if temp is not None:
                        recent_temps.append(temp)
                
                if recent_temps:
                    avg_temp = sum(recent_temps) / len(recent_temps)
                    current_temp = current_data.averages.temperature
                    
                    if current_temp > avg_temp + 2:
                        insights.append("температура выше среднего значения")
                    elif current_temp < avg_temp - 2:
                        insights.append("температура ниже среднего значения")
                    else:
                        insights.append("температура в пределах нормы")
                    
                    temperature_trend = analytics_pb2.TemperatureTrend(
                        current=current_temp,
                        recent_average=round(avg_temp, 2),
                        data_points=len(recent_temps)
                    )
            
            response = analytics_pb2.AnalyzeResponse(
                city=city,
                analysis_time=datetime.now().isoformat(),
                total_requests=len(city_history),
                current_weather=current_weather,
                insights=insights
            )
            
            if temperature_trend:
                response.temperature_trend.CopyFrom(temperature_trend)
            
            return response
            
        except grpc.RpcError as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Failed to get processed data: {str(e)}")
            return analytics_pb2.AnalyzeResponse()
    
    def GetHistory(self, request, context):
        city = request.city
        city_history = [item for item in weather_history if item['city'].lower() == city.lower()]
        
        # конвертируем последние 10 записей в JSON
        history_json = []
        for record in city_history[-10:]:
            history_json.append(json.dumps(record))
        
        return analytics_pb2.HistoryResponse(
            city=city,
            total_records=len(city_history),
            history=history_json
        )
    
    def HealthCheck(self, request, context):
        return analytics_pb2.HealthResponse(
            status="healthy",
            service="analytics"
        )

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    analytics_pb2_grpc.add_AnalyticsServiceServicer_to_server(
        AnalyticsService(), server
    )
    server.add_insecure_port('[::]:50053')
    
    print("gRPC Analytics Service запущен на порту 50053...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
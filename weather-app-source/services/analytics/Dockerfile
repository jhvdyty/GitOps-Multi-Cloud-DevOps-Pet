FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY generated/ ./generated/
COPY . .

EXPOSE 5003

# НЕ запускать генерацию, используем готовые файлы
CMD ["python", "app.py"]

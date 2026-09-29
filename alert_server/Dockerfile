FROM python:3.14-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY alert_server ./alert_server
COPY quality-rules ./quality-rules

EXPOSE 8000

CMD ["uvicorn", "alert_server.main:app", "--host", "0.0.0.0", "--port", "8000"]

# LOCAL DEV ONLY mock IPAM. Build from this folder:
#   docker build -t segment-generator-mock:dev dev/mock-generator
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY app.py /app/app.py

EXPOSE 9000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "9000"]

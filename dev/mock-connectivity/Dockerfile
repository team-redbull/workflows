# Mock of the external "next" connectivity service, for e2e testing where the
# real next service is unreachable (see helm/mock-connectivity/). Build from
# the REPO ROOT, like activities/connectivity/Dockerfile:
#   docker build -f dev/mock-connectivity/Dockerfile -t mock-connectivity:dev .
FROM python:3.12-slim

WORKDIR /app
COPY dev/mock-connectivity/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY dev/mock-connectivity/app.py /app/app.py

EXPOSE 9000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "9000"]

# Mock of the external "next" connectivity service, for e2e testing where the
# real next service is unreachable (see helm/mock-segment-connectivity/). Build from
# the REPO ROOT, like activities/segment_connectivity/Dockerfile:
#   docker build -f dev/mock-segment-connectivity/Dockerfile -t mock-segment-connectivity:dev .
FROM python:3.12-slim

WORKDIR /app
COPY dev/mock-segment-connectivity/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY dev/mock-segment-connectivity/app.py /app/app.py

EXPOSE 9000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "9000"]

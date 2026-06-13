FROM python:3.12-slim

WORKDIR /app

COPY activities/segment_allocation/requirements.txt /app/activities/segment_allocation/requirements.txt
RUN pip install --no-cache-dir -r /app/activities/segment_allocation/requirements.txt

COPY shared/ /app/shared/
COPY activities/ /app/activities/

ENV PYTHONPATH=/app
CMD ["python", "-m", "activities.segment_allocation.main_worker"]

FROM python:3.11-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre cron \
    && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt /app/requirements.txt
COPY backend/requirements-job.txt /app/requirements-job.txt
RUN pip install --no-cache-dir -r /app/requirements.txt -r /app/requirements-job.txt

COPY backend/app /app/app
COPY backend/scripts /app/scripts
COPY infra/scripts/run_case_packet_job.sh /app/run_case_packet_job.sh
COPY infra/scripts/cron/case_packet /etc/cron.d/case_packet

RUN chmod +x /app/run_case_packet_job.sh \
    && chmod 0644 /etc/cron.d/case_packet \
    && crontab /etc/cron.d/case_packet

ENV PYTHONPATH=/app
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV DATA_BASE_PATH=/data

CMD ["cron", "-f"]

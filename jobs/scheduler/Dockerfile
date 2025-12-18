FROM python:3.12-slim-bookworm
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    cron postgresql-client openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyspark==3.5.1 pandas pyarrow psycopg2-binary vaderSentiment

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYSPARK_PYTHON=python
ENV SPARK_LOCAL_IP=0.0.0.0

COPY batch/ /app/batch/
COPY serving/ /app/serving/

COPY jobs/scheduler/run_all.sh /app/run_all.sh
COPY jobs/scheduler/crontab /etc/cron.d/gabi-cron

RUN chmod +x /app/run_all.sh \
    && chmod 0644 /etc/cron.d/gabi-cron \
    && crontab /etc/cron.d/gabi-cron

CMD ["cron", "-f"]
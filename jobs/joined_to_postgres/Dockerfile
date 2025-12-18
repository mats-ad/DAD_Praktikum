FROM python:3.12-slim-bookworm

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    procps \
  && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyspark==3.5.1 pandas pyarrow psycopg2-binary

COPY batch /app/batch

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYSPARK_PYTHON=python
ENV SPARK_LOCAL_IP=0.0.0.0

CMD ["python", "-u", "/app/batch/load_joined_to_postgres.py"]
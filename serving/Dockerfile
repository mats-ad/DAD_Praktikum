FROM python:3.12-slim
WORKDIR /app
RUN pip install fastapi uvicorn psycopg2-binary
COPY serving/api.py /app/api.py
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
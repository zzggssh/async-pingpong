FROM python:3.10-slim
WORKDIR /app
COPY . /app
ENV PYTHONUNBUFFERED=1
CMD ["python3", "scripts/run_all.py", "--duration", "300"]

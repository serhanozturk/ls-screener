FROM python:3.12-slim
WORKDIR /app
COPY Screener.py .
EXPOSE 8766
CMD ["python", "Screener.py"]

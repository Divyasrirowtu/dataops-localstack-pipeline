FROM python:3.10-slim

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ETL script
COPY etl.py .

# Run ETL
CMD ["python", "etl.py"]

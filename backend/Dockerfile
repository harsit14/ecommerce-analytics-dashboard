# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY main.py .
COPY database.py .
COPY models.py .

# Expose port (Cloud Run will set PORT env var)
EXPOSE 8080

# Run the application
# Cloud Run will inject PORT environment variable
CMD exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}

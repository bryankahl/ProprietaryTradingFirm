# Base image: Lightweight Python 3.10
FROM python:3.10-slim

# Install GCC (required for building some Python extensions like asyncpg/numpy)
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Default command to run the trading bot
CMD ["python", "main.py"]
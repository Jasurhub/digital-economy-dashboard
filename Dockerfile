FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for lxml
RUN apt-get update && apt-get install -y \
    libxml2-dev libxslt-dev gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create volume mount point for persistent DB
RUN mkdir -p /data
ENV DB_PATH=/data/digital_economy.db

EXPOSE 8000

# SQLite is single-writer; 1 worker avoids all locking races.
# Scale horizontally via multiple containers + a shared volume if needed.
CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]

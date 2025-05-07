# Stage 1: Build stage
FROM dhub.pubalibankbd.com/python/python:3.11 AS builder

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt --trusted-host=pypi.org --trusted-host=files.pythonhosted.org

# Stage 2: Final stage
FROM dhub.pubalibankbd.com/python/python:3.11-slim

WORKDIR /app

# Install netcat for database connectivity checks
RUN apt-get update && apt-get install -y netcat-openbsd && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

COPY . .

RUN chmod +x /app/entrypoint.sh

EXPOSE 5000

ENTRYPOINT ["/app/entrypoint.sh"]


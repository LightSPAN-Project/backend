FROM python:3.11-slim

# Avoid Python buffering issues
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# System deps (needed for pandas/dateutil sometimes)
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app code
COPY . .

# Streamlit on Cloud Run
EXPOSE 8080
ENV PORT=8080

CMD streamlit run app.py \
  --server.port=$PORT \
  --server.address=0.0.0.0 \
  --server.enableCORS=false \
  --server.enableXsrfProtection=false

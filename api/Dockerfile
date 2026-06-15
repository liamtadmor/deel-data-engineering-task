FROM python:3.14-slim

WORKDIR /app

# Install system dependencies required for compiling psycopg2 if needed
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the API source code
COPY . .

# Expose port 8000 for the FastAPI container
EXPOSE 8000

# Command to launch the API service
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
# Base image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy all files
COPY . .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create required directories
RUN mkdir -p datalake/raw datalake/refined datalake/consumption/plots

# Run pipeline
CMD ["python", "pipeline/main.py"]
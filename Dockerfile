FROM python:3.11-slim

WORKDIR /app

# Copy only dependency files first
COPY pyproject.toml ./

# Install dependencies using pip
RUN pip install --no-cache-dir .

# Install Playwright browsers and system dependencies
RUN playwright install --with-deps chromium

# Copy application code (this layer changes more frequently)
COPY crawlers ./crawlers
COPY api.py ./

# Create data directory
RUN mkdir -p /app/data

# Set environment variables
ENV PYTHONUNBUFFERED=1

CMD ["python", "api.py"]

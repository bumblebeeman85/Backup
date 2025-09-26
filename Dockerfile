FROM python:3.11-slim

# Create working directory
WORKDIR /app

# Install system deps (if needed) and Python deps
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy package
COPY src /app/src
ENV PYTHONPATH=/app/src

# Default backup dir inside container
ENV BACKUP_DIR=/data/m365_mail_backups
VOLUME ["/data"]

# Expose web GUI port
EXPOSE 6666

# Add a small scheduler runner script
COPY scripts/scheduler_runner.py /app/scripts/scheduler_runner.py

# Default command: run uvicorn serving the web GUI on port 6666
CMD ["uvicorn", "m365_backup.web:app", "--host", "0.0.0.0", "--port", "6666"]

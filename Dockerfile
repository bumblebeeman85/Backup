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

# Default command: run backup using tenants.yaml in the mounted working dir
CMD ["python", "-m", "m365_backup.main", "backup", "--tenants", "tenants.yaml"]

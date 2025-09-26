

# M365 Iron Backup

Backup and restore Microsoft 365 mailboxes for multiple tenants, with global deduplication and scheduled snapshots. Includes a web GUI and Postgres 17 support.

## Requirements (Debian 13)
- Python 3.11+
- PostgreSQL 17
- pip (Python package manager)
- git (for cloning the repo)


## Docker Installation & Usage

### Prerequisites
- Docker (https://docs.docker.com/get-docker/)
- Docker Compose (included in Docker Desktop or install via package manager)

### 1. Install Docker & Docker Compose
On Debian:
```bash
sudo apt update
sudo apt install docker.io docker-compose git
sudo systemctl enable --now docker
```
Verify installation:
```bash
docker --version
docker-compose --version
```

### 2. Clone the repository
```bash
git clone https://github.com/bumblebeeman85/Backup.git
cd Backup
```

### 3. Prepare configuration
- Copy `tenants.example.yaml` to `tenants.yaml` and fill in your tenant info.
- Store secrets in environment variables or `tenants.secret.yaml` (never commit secrets).

### 4. Build and start the stack
```bash
docker-compose up --build
```

# M365 Iron Backup

Backup and restore Microsoft 365 mailboxes for multiple tenants, with global deduplication and scheduled snapshots. Includes a web GUI and Postgres 17 support.

---

## Option 1: Bare Metal Installation (Debian 13 or similar)

### Requirements
- Python 3.11+
- PostgreSQL 17
- pip (Python package manager)
- git (for cloning the repo)

### 1. Install system packages:
```bash
sudo apt update
sudo apt install python3 python3-venv python3-pip postgresql-17 git
```

### 2. Clone the repository and enter the project folder:
```bash
git clone https://github.com/bumblebeeman85/Backup.git
cd Backup
```

### 3. Set up Python environment and install dependencies:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 4. Set up Postgres 17:
```bash
sudo -u postgres psql
CREATE DATABASE m365backup;
\q
```
You may need to set a password for the `postgres` user and update `DATABASE_URL` in your environment or `.env` file.

### 5. Prepare configuration
- Copy `tenants.example.yaml` to `tenants.yaml` and fill in your tenant info.
- Store secrets in environment variables or `tenants.secret.yaml` (never commit secrets).

### 6. Running the App
- Run a backup manually:
  ```bash
  python -m m365_backup.main backup --tenants tenants.yaml --mails-per-user 100
  ```
- Run the web GUI (port 6666):
  ```bash
  python -m m365_backup.main web
  # Then open http://localhost:6666 in your browser
  ```
- Run the scheduler (snapshots 4x daily):
  ```bash
  python scripts/scheduler_runner.py
  ```
- Run a one-shot snapshot:
  ```bash
  python -m m365_backup.main snapshot
  ```

---

## Option 2: Docker/Docker Compose Installation

### Prerequisites
- Docker (https://docs.docker.com/get-docker/)
- Docker Compose (included in Docker Desktop or install via package manager)

### 1. Install Docker & Docker Compose
On Debian:
```bash
sudo apt update
sudo apt install docker.io docker-compose git
sudo systemctl enable --now docker
```
Verify installation:
```bash
docker --version
docker-compose --version
```

### 2. Clone the repository
```bash
git clone https://github.com/bumblebeeman85/Backup.git
cd Backup
```

### 3. Prepare configuration
- Copy `tenants.example.yaml` to `tenants.yaml` and fill in your tenant info.
- Store secrets in environment variables or `tenants.secret.yaml` (never commit secrets).

### 4. Build and start the stack
```bash
docker-compose up --build
```
This will:
- Start Postgres 17 (service: `db`)
- Build and run the web GUI (service: `app`, port 6666)
- Start the scheduler (service: `scheduler`, runs snapshot jobs 4x daily)

Web GUI will be available at [http://localhost:6666](http://localhost:6666)

### 5. Common Docker commands
- To run only the web app:
  ```bash
  docker-compose up --build app
  ```
- To run only the scheduler:
  ```bash
  docker-compose up --build scheduler
  ```
- To run a one-shot snapshot:
  ```bash
  docker-compose run --rm app python -m m365_backup.main snapshot
  ```

### 6. Environment variables
You can pass secrets and config via environment variables in `docker-compose.yml` or with `-e` flags:
```yaml
    environment:
      CLIENT_ID: your_client_id
      CLIENT_SECRET: your_client_secret
      TENANT_ID: your_tenant_id
      DATABASE_URL: postgresql+asyncpg://postgres:postgres@db:5432/m365backup
```

### 7. Data persistence
- Backups and database data are stored in Docker volumes (`m365_backups`, `m365_db_data`).
- To access backup files, mount a local directory or use `docker cp`.

### 8. Troubleshooting
- If you see import errors, rebuild the image:
  ```bash
  docker-compose build --no-cache
  docker-compose up
  ```
- For Postgres connection errors, ensure the `db` service is healthy and environment variables are correct.
- For permission errors, run Docker commands with `sudo` or add your user to the `docker` group:
  ```bash
  sudo usermod -aG docker $USER
  newgrp docker
  ```

### 9. Stopping and cleaning up
```bash
docker-compose down
```
To remove all volumes (including backups and DB data):
```bash
docker-compose down -v
```

---

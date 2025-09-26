# M365 Iron Backup

This project provides a simple tool to backup Microsoft 365 mailboxes (messages + attachments) for one or more tenants.

Requirements
- Python 3.11+
- Docker (optional)

Quick start (local)
1. Create a `tenants.yaml` based on `tenants.example.yaml` and place it next to this README.
2. Set secrets as environment variables or in the tenants file (not recommended):

```bash
export CLIENT_ID=...
export CLIENT_SECRET=...
export TENANT_ID=... # optional per-tenant override
```

3. Install dependencies and run:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m m365_backup.main backup --tenants tenants.yaml --mails-per-user 100
```

Run with Docker

```bash
docker build -t m365-backup:latest .
# mount a directory with tenants.yaml and where backups are stored
docker run --rm -v $(pwd)/data:/data -v $(pwd)/tenants.yaml:/app/tenants.yaml \
  -e CLIENT_ID=... -e CLIENT_SECRET=... m365-backup:latest
```

Notes
- Do not commit secrets into version control.
- Restore functionality is a placeholder and not implemented in v0.1.

#!/usr/bin/env python3
"""Run the snapshot scheduler alongside the web app.

This lightweight runner imports the app and starts the scheduler in the main thread.
It's intended to be run in the container as a background process if desired.
"""
import os
import logging
from m365_backup import main
from m365_backup import db
from m365_backup.scheduler import start_scheduler

logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'INFO'))
logger = logging.getLogger('scheduler_runner')


def snapshot_job():
    # load tenants and run a single snapshot storing results to DB
    tenants_file = os.environ.get('TENANTS_FILE', 'tenants.yaml')
    try:
        tenants = main.load_tenants(tenants_file)
    except Exception:
        logger.exception('Failed to load tenants')
        return

    for tenant in tenants:
        collected = main.backup_tenant(tenant, {'mails_per_user': int(os.environ.get('MAILS_PER_USER', '200')), 'download_attachments': True})
        if collected:
            db.init_db()
            sid, inserted = db.store_snapshot('scheduled', collected)
            logger.info('Stored snapshot %s with %d new messages', sid, inserted)


if __name__ == '__main__':
    # start scheduler with default 4x daily
    start_scheduler(snapshot_job)
    # Keep the process alive
    import time
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        logger.info('Scheduler runner stopping')

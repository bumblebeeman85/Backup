from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import logging
from typing import Callable

logger = logging.getLogger("m365_backup.scheduler")


def start_scheduler(job_func: Callable, cron_expressions: list = None):
    """Start APScheduler to run job_func on provided cron times.

    cron_expressions: list of dicts for apscheduler add_job kwargs, e.g. {'hour': '0,6,12,18'}
    """
    sched = BackgroundScheduler()
    # default: run 4x daily at 00:00,06:00,12:00,18:00
    if not cron_expressions:
        cron_expressions = [{"hour": "0,6,12,18", "minute": "0"}]

    for expr in cron_expressions:
        sched.add_job(
            job_func,
            "cron",
            **expr,
            id=f'm365_backup_{expr.get("hour")}_{expr.get("minute","0")}',
        )
        logger.info("Scheduled job with %s", expr)

    sched.start()
    logger.info("Scheduler started at %s", datetime.utcnow())
    return sched

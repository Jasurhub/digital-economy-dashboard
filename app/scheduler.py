"""
Background scheduler – runs the scraper periodically.
Default: every 6 hours.
"""
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

logger = logging.getLogger("scheduler")
_scheduler = None


def _job():
    logger.info("Scheduled scrape starting...")
    try:
        from app.scraper import run_scraper
        count = run_scraper()
        logger.info(f"Scheduled scrape done. {count} rows updated.")
    except Exception as e:
        logger.error(f"Scheduled scrape failed: {e}", exc_info=True)


def start_scheduler(hours: int = 6):
    global _scheduler
    if _scheduler and _scheduler.running:
        return
    _scheduler = BackgroundScheduler(timezone="Asia/Tashkent")
    _scheduler.add_job(_job, IntervalTrigger(hours=hours), id="scrape_job", replace_existing=True)
    _scheduler.start()
    logger.info(f"Scheduler started – scrape every {hours}h (Asia/Tashkent)")


def stop_scheduler():
    global _scheduler
    if _scheduler:
        _scheduler.shutdown(wait=False)

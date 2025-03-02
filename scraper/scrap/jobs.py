from apscheduler.schedulers.background import BackgroundScheduler
from .stock_scraper import scrape_nepse_data
from apscheduler.triggers.interval import IntervalTrigger
def start_jobs():
    scheduler = BackgroundScheduler()
    scheduler.add_job(scrape_nepse_data, IntervalTrigger(minutes=5))
    scheduler.start()
    print("Scheduler started")

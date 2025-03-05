from apscheduler.schedulers.background import BackgroundScheduler
from .stock_scraper import scrape_nepse_data
import time

def start_jobs():
    scheduler = BackgroundScheduler()
    # Schedule the job to run 
    scheduler.add_job(scrape_nepse_data)
    scheduler.start()
    print("Scheduler started")


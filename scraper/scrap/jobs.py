from apscheduler.schedulers.background import BackgroundScheduler
from .scraper_TP import scrape_nepse_data
from .scraper_FS import scrape_nepse_FS

def start_jobs():
    scheduler = BackgroundScheduler()
    # Schedule the job to run 
    scheduler.add_job(scrape_nepse_data)
    # scheduler.add_job(scrape_nepse_FS)
    scheduler.start()
    print("Scheduler started")


from django.apps import AppConfig
import os

class ScrapConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'scrap'

    # def ready(self):
    #     if os.environ.get("RUN_MAIN",None) != 'true':
    #         return
    #     from .jobs import start_jobs
    #     start_jobs()
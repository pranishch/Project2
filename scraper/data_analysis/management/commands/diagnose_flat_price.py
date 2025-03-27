from django.core.management.base import BaseCommand
import logging
from data_analysis.models import FloorsheetData

class Command(BaseCommand):
    help = 'Diagnose flat price detection data issues'

    def handle(self, *args, **options):
        logging.basicConfig(level=logging.DEBUG)
        logger = logging.getLogger('flat_price_detection_diagnosis')

        try:
            # Check total records
            total_records = FloorsheetData.objects.count()
            self.stdout.write(self.style.SUCCESS(f"Total FloorsheetData records: {total_records}"))

            # Check date range
            earliest_date = FloorsheetData.objects.earliest('date').date
            latest_date = FloorsheetData.objects.latest('date').date
            self.stdout.write(self.style.SUCCESS(f"Date Range: {earliest_date} to {latest_date}"))

            # Sample data retrieval
            sample_data = FloorsheetData.objects.filter(
                date__range=[earliest_date, latest_date]
            ).values('symbol', 'rate', 'transaction_no', 'date')[:10]
            
            self.stdout.write("Sample Data:")
            for record in sample_data:
                self.stdout.write(str(record))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Diagnosis failed: {str(e)}"))
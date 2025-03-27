from django.core.management.base import BaseCommand
import csv
from datetime import datetime
from data_analysis.models import FloorsheetData

class Command(BaseCommand):
    help = 'Import floorsheet data from CSV file'

    def handle(self, *args, **options):
        FloorsheetData.objects.all().delete()
        # Define the CSV file path directly in the program
        csv_file_path = r'C:\Users\Arjun\Desktop\project2\scraper\data_analysis\floorsheet_floorsheetdata.csv'
        
        self.stdout.write(self.style.SUCCESS(f'Starting import from {csv_file_path}'))
        
        rows_imported = 0
        rows_failed = 0
        
        try:
            with open(csv_file_path, 'r') as file:
                # Use csv.reader with comma delimiter and skip header
                reader = csv.reader(file, delimiter=',')
                next(reader)  # Skip header row
                
                for row in reader:
                    try:
                        if len(row) < 9:
                            self.stdout.write(self.style.WARNING(f'Skipping row with insufficient data: {row}'))
                            rows_failed += 1
                            continue
                        
                        # Parse date from DD/MM/YYYY to YYYY-MM-DD
                        date_str = row[8]
                        
                        
                        # Handle scientific notation and remove commas for numeric fields
                        transaction_no = "{:.0f}".format(float(row[1]))  # Convert scientific notation to integer
                        quantity = float(row[5].replace(',', ''))
                        # Remove commas from rate and amount before converting to float
                        rate = float(row[6].replace(',', ''))
                        amount = float(row[7].replace(',', ''))
                        
                        # Create model instance
                        FloorsheetData.objects.create(
                            transaction_no=transaction_no,
                            symbol=row[2],
                            buyer=row[3],
                            seller=row[4],
                            quantity=quantity,
                            rate=rate,
                            amount=amount,
                            date=date_str
                        )
                        
                        rows_imported += 1
                        
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f'Error importing row: {row}'))
                        self.stdout.write(self.style.ERROR(f'Exception: {e}'))
                        rows_failed += 1
                        
            self.stdout.write(self.style.SUCCESS(f'Import completed. {rows_imported} rows imported successfully.'))
            
            if rows_failed > 0:
                self.stdout.write(self.style.WARNING(f'{rows_failed} rows failed to import.'))
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Failed to import data: {e}'))
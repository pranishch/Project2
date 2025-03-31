import os
import csv
from django.core.management.base import BaseCommand
from django.db import transaction
from data_analysis.models import SectorData
from datetime import datetime

class Command(BaseCommand):
    help = 'Imports sector data from CSV file into database'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--file',
            type=str,
            default='C:\\Users\\Arjun\\Desktop\\project2\\scraper\\data_analysis\\sector_data.csv',
            help='Path to the CSV file (default: C:\\Users\\Arjun\\Desktop\\project2\\scraper\\data_analysis\\sector_data.csv)'
        )
        parser.add_argument(
            '--clear',
            action='store_true',
            help='Clear existing data before import'
        )

    def handle(self, *args, **options):
        file_path = options['file']
        
        if not os.path.exists(file_path):
            self.stdout.write(self.style.ERROR(f"File not found: {file_path}"))
            return
            
        if options['clear']:
            self.stdout.write("Clearing existing sector data...")
            SectorData.objects.all().delete()
            
        self.stdout.write(f"Importing data from {file_path}...")
        
        imported_count = 0
        skipped_count = 0
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            # Try to auto-detect CSV dialect
            try:
                dialect = csv.Sniffer().sniff(csvfile.read(1024))
                csvfile.seek(0)
                reader = csv.DictReader(csvfile, dialect=dialect)
            except:
                csvfile.seek(0)
                reader = csv.DictReader(csvfile)
            
            required_fields = ['sector_description', 'symbol']
            if not all(field in reader.fieldnames for field in required_fields):
                self.stdout.write(self.style.ERROR(
                    f"CSV file must contain these columns: {', '.join(required_fields)}"
                ))
                return
                
            with transaction.atomic():
                for row_num, row in enumerate(reader, start=1):
                    try:
                        # Skip empty rows
                        if not any(row.values()):
                            continue
                            
                        # Clean data
                        symbol = row['symbol'].strip() if row['symbol'] else None
                        description = row['sector_description'].strip() if row['sector_description'] else None
                        
                        # Skip if both fields are empty
                        if not symbol and not description:
                            skipped_count += 1
                            continue
                            
                        # Create or update record
                        SectorData.objects.update_or_create(
                            symbol=symbol,
                            defaults={
                                'sector_description': description
                            }
                        )
                        imported_count += 1
                        
                    except Exception as e:
                        self.stdout.write(
                            self.style.WARNING(f"Error processing row {row_num}: {str(e)}")
                        )
                        skipped_count += 1
                        continue
                        
        self.stdout.write(self.style.SUCCESS(
            f"Import completed. {imported_count} records imported, {skipped_count} skipped"
        ))
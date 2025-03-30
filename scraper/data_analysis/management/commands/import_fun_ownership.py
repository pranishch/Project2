from django.core.management.base import BaseCommand
import csv
from datetime import datetime
from data_analysis.models import FunOwnership

class Command(BaseCommand):
    help = 'Import FUN ownership data from CSV file'

    def handle(self, *args, **options):
        csv_file_path = r'C:\Users\Arjun\Desktop\project2\scraper\data_analysis\fun_ownership.csv'
        
        self.stdout.write(self.style.SUCCESS(f'Starting import from {csv_file_path}'))
        
        rows_imported = 0
        rows_failed = 0
        
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                
                for row in reader:
                    try:
                        # Normalize column names by stripping whitespace
                        row = {k.strip().replace(' ', '_'): v for k, v in row.items()}
                        
                        # Validate required fields
                        required_fields = ['script_id', 'updated_on', 'promoter_shares', 
                                         'public__shares', 'total_listed_shares', 'symbol']
                        if not all(field in row for field in required_fields):
                            missing = [f for f in required_fields if f not in row]
                            self.stdout.write(self.style.WARNING(
                                f'Skipping row - missing fields: {missing}. Row data: {row}'
                            ))
                            rows_failed += 1
                            continue
                        
                        # Parse date (handling "Oct 19, 2023" format)
                        date_str = row['updated_on'].strip()
                        date_formats = ['%b %d, %Y', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y']
                        date_obj = None
                        
                        for fmt in date_formats:
                            try:
                                date_obj = datetime.strptime(date_str, fmt).date()
                                break
                            except ValueError:
                                continue
                        
                        if not date_obj:
                            self.stdout.write(self.style.WARNING(
                                f'Unparseable date format: {date_str} in row: {row}'
                            ))
                            rows_failed += 1
                            continue
                        
                        # Clean and convert numeric values
                        def clean_number(num_str):
                            if isinstance(num_str, str):
                                return int(float(num_str.replace(',', '').strip() or 0))
                            return int(num_str or 0)
                        
                        # Create or update record
                        FunOwnership.objects.update_or_create(
                            script_id=row['script_id'].strip(),
                            updated_on=date_obj,
                            defaults={
                                'promoter_shares': clean_number(row['promoter_shares']),
                                'public_shares': clean_number(row['public__shares']),
                                'total_listed_shares': clean_number(row['total_listed_shares']),
                                'symbol': row['symbol'].strip().upper()
                            }
                        )
                        rows_imported += 1
                        
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f'Error importing row: {row}'))
                        self.stdout.write(self.style.ERROR(f'Exception: {str(e)}'))
                        rows_failed += 1
                        
            self.stdout.write(self.style.SUCCESS(
                f'Import completed. Success: {rows_imported}, Failed: {rows_failed}'
            ))
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Failed to process CSV file: {str(e)}'))
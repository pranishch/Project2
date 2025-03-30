import datetime
import pandas as pd
from django.core.management.base import BaseCommand
from django.db import transaction
from ...models import FloorsheetData, ConsecutiveNoTrade

class Command(BaseCommand):
    help = 'Identifies stocks with consecutive days without trading activity'

    def add_arguments(self, parser):
        parser.add_argument(
            '--min-days',
            type=int,
            default=5,
            help='Minimum number of consecutive no-trade days to flag (default: 5)'
        )
        parser.add_argument(
            '--date',
            type=str,
            help='Reference date for the report (YYYY-MM-DD). Defaults to today.'
        )

    def handle(self, *args, **options):
        min_days = options['min_days']
        reference_date = options['date']
        
        # Convert reference date to datetime.date
        if reference_date:
            try:
                reference_date = datetime.datetime.strptime(reference_date, '%Y-%m-%d').date()
            except ValueError:
                self.stdout.write(self.style.ERROR('Invalid date format. Use YYYY-MM-DD.'))
                return
        else:
            reference_date = datetime.date.today()

        self.stdout.write(f"Identifying stocks with {min_days}+ consecutive no-trade days as of {reference_date}")

        try:
            # Step 1: Get data with strict date conversion
            self.stdout.write("Fetching and processing data...")
            
            # Get data from database and ensure proper date conversion
            qs = FloorsheetData.objects.all().values('symbol', 'date')
            df = pd.DataFrame.from_records(qs)
            
            # Convert all dates to datetime.date objects
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
            
            # Remove rows with invalid dates or symbols
            df = df.dropna(subset=['date', 'symbol'])
            
            # Step 2: Process each symbol
            results = []
            
            # Group by symbol for efficient processing
            grouped = df.groupby('symbol')
            
            for symbol, group in grouped:
                if not symbol:  # Skip empty symbols
                    continue
                    
                # Get unique sorted dates for this symbol
                traded_dates = sorted(set(d for d in group['date'] if isinstance(d, datetime.date)))
                
                if not traded_dates:  # Skip if no valid dates
                    continue
                
                # Find gaps
                gaps = self.find_date_gaps(traded_dates, reference_date)
                
                # Filter by minimum days
                for gap in gaps:
                    if gap['length'] >= min_days:
                        results.append({
                            'symbol': str(symbol),
                            'start_date': gap['start'],
                            'end_date': gap['end'],
                            'days_count': gap['length'],
                            'calculated_date': reference_date
                        })
            
            # Step 3: Save results
            self.save_results(results)
            
            self.stdout.write(self.style.SUCCESS(
                f"Successfully processed {len(grouped)} symbols. Found {len(results)} with {min_days}+ consecutive no-trade days."
            ))
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error processing report: {str(e)}"))
            raise

    def find_date_gaps(self, dates, reference_date):
        """
        Identify gaps between traded dates for a stock.
        All inputs must be datetime.date objects.
        """
        gaps = []
        
        # 1. Validate all dates are datetime.date objects
        if not all(isinstance(d, datetime.date) for d in dates):
            raise ValueError("All dates must be datetime.date objects")
        
        # 2. Gap before first trade date (if any)
        first_date = dates[0]
        if isinstance(first_date, datetime.date) and reference_date > first_date:
            gaps.append({
                'start': first_date + datetime.timedelta(days=1),
                'end': reference_date,
                'length': (reference_date - first_date).days
            })
        
        # 3. Gaps between trades
        for i in range(1, len(dates)):
            prev_date = dates[i-1]
            current_date = dates[i]
            
            if not isinstance(prev_date, datetime.date) or not isinstance(current_date, datetime.date):
                continue
                
            gap_length = (current_date - prev_date).days - 1
            
            if gap_length > 0:
                gaps.append({
                    'start': prev_date + datetime.timedelta(days=1),
                    'end': current_date - datetime.timedelta(days=1),
                    'length': gap_length
                })
        
        return gaps

    @transaction.atomic
    def save_results(self, results):
        """Save results to database with type validation"""
        ConsecutiveNoTrade.objects.all().delete()
        
        records = []
        for r in results:
            try:
                records.append(ConsecutiveNoTrade(
                    symbol=str(r['symbol']),
                    start_date=r['start_date'],
                    end_date=r['end_date'],
                    days_count=r['days_count'],
                    calculated_date=r['calculated_date']
                ))
            except (ValueError, TypeError) as e:
                self.stdout.write(self.style.WARNING(
                    f"Skipping invalid record: {r}. Error: {str(e)}"
                ))
        
        ConsecutiveNoTrade.objects.bulk_create(records)
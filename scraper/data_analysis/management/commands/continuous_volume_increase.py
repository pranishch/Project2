import django
django.setup()

from django.core.management.base import BaseCommand
from django.db import transaction
from data_analysis.models import FloorsheetData, ContinuousVolumeIncrease  # Replace 'your_app' with your actual app name
import dask.dataframe as dd
from dask.distributed import Client
from datetime import datetime, timedelta
import pandas as pd

class Command(BaseCommand):
    help = 'Identify stocks with volume increasing for 3+ consecutive days using Dask'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Starting continuous volume increase analysis...'))
        
        # Initialize Dask client
        client = Client()
        
        try:
            # Process data and save results
            self.process_volume_increase()
            self.stdout.write(self.style.SUCCESS('Successfully completed continuous volume increase analysis.'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error: {str(e)}'))
        finally:
            client.close()

    def process_volume_increase(self):
        # Get the date range (last 30 days for analysis)
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=240)
        
        # Query the data using Django ORM and convert to Dask dataframe
        self.stdout.write("Fetching data from database...")
        queryset = FloorsheetData.objects.filter(
            date__gte=start_date,
            date__lte=end_date,
            symbol__isnull=False  # Ensure we only get records with symbols
        ).values('symbol', 'date', 'quantity')
        
        # Convert to pandas DataFrame first
        df = pd.DataFrame.from_records(queryset)
        
        # Check if dataframe is empty
        if df.empty:
            self.stdout.write(self.style.WARNING("No data found for the specified date range."))
            return
            
        # Ensure required columns exist
        if not all(col in df.columns for col in ['symbol', 'date', 'quantity']):
            self.stdout.write(self.style.ERROR("Required columns (symbol, date, quantity) not found in data."))
            return
        
        # Convert date column to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(df['date']):
            df['date'] = pd.to_datetime(df['date']).dt.date
        
        ddf = dd.from_pandas(df, npartitions=4)
        
        self.stdout.write("Processing data...")
        # Group by symbol and date, sum quantities
        daily_volume = ddf.groupby(['symbol', 'date']).quantity.sum().reset_index()
        
        # Sort by symbol and date
        daily_volume = daily_volume.compute().sort_values(['symbol', 'date'])
        
        # Find consecutive increasing volume days
        result = self.find_consecutive_increases(daily_volume)
        
        # Save results to database
        self.save_results(result)
    
    def find_consecutive_increases(self, daily_volume):
        self.stdout.write("Identifying consecutive volume increases...")
        results = []
        
        # Ensure we have the required columns
        if not all(col in daily_volume.columns for col in ['symbol', 'date', 'quantity']):
            self.stdout.write(self.style.ERROR("Required columns missing in daily volume data."))
            return results
        
        # Ensure date is in datetime format
        if not pd.api.types.is_datetime64_any_dtype(daily_volume['date']):
            daily_volume['date'] = pd.to_datetime(daily_volume['date'])
        
        # Process each symbol
        for symbol, group in daily_volume.groupby('symbol'):
            if pd.isna(symbol):  # Skip null symbols
                continue
                
            group = group.sort_values('date')
            
            # Skip symbols with insufficient data (less than 3 days)
            if len(group) < 3:
                continue
                
            # Calculate day-to-day volume change
            group['volume_change'] = group['quantity'].diff()
            group['increasing'] = group['volume_change'] > 0
            
            # Find consecutive increasing days
            group['consecutive_increase'] = (group['increasing'].astype(int)
                                           .groupby((~group['increasing']).cumsum())
                                           .cumcount() + 1)
            
            # Filter for streaks of 3+ days
            streaks = group[group['consecutive_increase'] >= 3]
            
            if not streaks.empty:
                # Get the latest streak for each symbol
                latest_streak = streaks.iloc[-1]
                # Convert numpy.int64 to Python int before using in timedelta
                consecutive_days = int(latest_streak['consecutive_increase'])
                # Ensure we're working with datetime.date object
                end_date = latest_streak['date'].date() if hasattr(latest_streak['date'], 'date') else latest_streak['date']
                start_date = end_date - timedelta(days=consecutive_days - 1)
                
                # Calculate percentage change safely
                try:
                    prev_volume = group.iloc[-2]['quantity'] if len(group) > 1 else 0
                    percentage_change = ((latest_streak['quantity'] - prev_volume) / prev_volume * 100) if prev_volume != 0 else 0
                except:
                    percentage_change = 0
                
                results.append({
                    'symbol': symbol,
                    'start_date': start_date,
                    'end_date': end_date,
                    'consecutive_days': consecutive_days,
                    'current_volume': latest_streak['quantity'],
                    'percentage_change': percentage_change
                })
        
        return results
    
    @transaction.atomic
    def save_results(self, results):
        self.stdout.write("Saving results to database...")
        
        # Clear existing data
        ContinuousVolumeIncrease.objects.all().delete()
        
        # Create new records
        records = [
            ContinuousVolumeIncrease(
                symbol=result['symbol'],
                start_date=result['start_date'],
                end_date=result['end_date'],
                consecutive_days=result['consecutive_days'],
                current_volume=result['current_volume'],
                percentage_change=result['percentage_change'],
            )
            for result in results
        ]
        
        ContinuousVolumeIncrease.objects.bulk_create(records)
        self.stdout.write(self.style.SUCCESS(f"Saved {len(records)} records to database."))
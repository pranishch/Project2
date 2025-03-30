from django.core.management.base import BaseCommand
from datetime import timedelta
from data_analysis.models import FloorsheetData, VolumeDriedUp

import pandas as pd

class Command(BaseCommand):
    help = 'Detect stocks with dried-up trading volume'

    def handle(self, *args, **options):
        self.stdout.write("Starting volume dried-up analysis...")
        
        # Parameters
        THRESHOLD = 0.2
        LOOKBACK_DAYS = 30
        
        # Delete previous results
        VolumeDriedUp.objects.all().delete()
        
        # Get the latest date and define the start date
        latest_date = FloorsheetData.objects.latest('date').date
        start_date = latest_date - timedelta(days=LOOKBACK_DAYS)
        
        # Query for records in the date range
        queryset = FloorsheetData.objects.filter(
            date__gte=start_date, date__lte=latest_date
        ).values('symbol', 'date', 'quantity')
        
        df = pd.DataFrame(list(queryset))
        
        # Process and save volume dried-up stocks
        self.calculate_and_save_volume_dried_up(df, THRESHOLD)
        
        self.stdout.write(self.style.SUCCESS('Analysis completed'))

    def calculate_and_save_volume_dried_up(self, df, THRESHOLD):
        daily_volume = df.groupby(['symbol', 'date'])['quantity'].sum().reset_index()
        
        # Compute past 7-day rolling average (excluding the current day)
        daily_volume['avg_volume_past_week'] = daily_volume.groupby('symbol')['quantity'].transform(
            lambda x: x.rolling(window=7, min_periods=1).mean().shift(1)
        )
        
        # Identify stocks where volume is below the threshold
        daily_volume['volume_dried_up'] = daily_volume['quantity'] < (THRESHOLD * daily_volume['avg_volume_past_week'])
        
        filtered_data = daily_volume[daily_volume['volume_dried_up']][['symbol', 'date', 'quantity', 'avg_volume_past_week']]
        
        records_to_save = []
        for _, row in filtered_data.iterrows():
            record = VolumeDriedUp(
                symbol=row['symbol'],
                date=row['date'],
                quantity=row['quantity'],
                avg_volume_past_week=row['avg_volume_past_week']
            )
            records_to_save.append(record)
            self.stdout.write(f"Processing symbol: {row['symbol']}, date: {row['date']}")
        
        # Save results in bulk
        VolumeDriedUp.objects.bulk_create(records_to_save)
        
        self.stdout.write(f"Completed processing. {len(records_to_save)} records saved.")
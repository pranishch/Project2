import django
django.setup()
from django.core.management.base import BaseCommand
from django.db import models
from data_analysis.models import FloorsheetData, BulkVolume
from datetime import datetime, timedelta
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd

class Command(BaseCommand):
    help = 'Process bulk volume trades to find all scripts traded by each broker'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Starting bulk volume trade processing...'))
        
        client = Client()
        try:
            # Process data for different time frames
            time_frames = [
                ('Daily', timedelta(days=1)),
                ('Weekly', timedelta(days=4)),
                ('Monthly', timedelta(days=29)),
                ('3 Month', timedelta(days=89)),
                ('6 Month', timedelta(days=179))
            ]
            
            for time_frame, delta in time_frames:
                self.process_time_frame(time_frame, delta)
            
            self.stdout.write(self.style.SUCCESS('Successfully processed bulk volume trades'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error: {str(e)}'))
        finally:
            client.close()

    def process_time_frame(self, time_frame, delta):
        self.stdout.write(f'\nProcessing {time_frame} time frame...')
        
        # Get the latest date
        latest_date = FloorsheetData.objects.aggregate(models.Max('date'))['date__max']
        if not latest_date:
            self.stdout.write(self.style.WARNING('No data available'))
            return
            
        # Calculate date range
        start_date = latest_date - delta
        
        # Format date range display
        date_range = (
            latest_date.strftime('%Y-%m-%d') if time_frame == 'Daily'
            else f"{start_date.strftime('%Y-%m-%d')} to {latest_date.strftime('%Y-%m-%d')}"
        )

        # Get data using Dask for better performance with large datasets
        queryset = FloorsheetData.objects.filter(
            date__range=(start_date, latest_date)
        ).values('symbol', 'buyer', 'quantity', 'date')

        # Convert to DataFrame
        df = dd.from_pandas(pd.DataFrame(list(queryset)), npartitions=10)
        
        # Step 1: Aggregate quantities for each buyer-symbol combination
        broker_script_volume = (
            df.groupby(['buyer', 'symbol'])
            .agg({'quantity': 'sum'})
            .reset_index()
            .compute()
        )
        
        # Sort by quantity in descending order
        broker_script_volume = broker_script_volume.sort_values('quantity', ascending=False)
        
        # Debug output
        self.stdout.write(f"Total broker-script combinations: {len(broker_script_volume)}")
        
        # Delete existing records for this time frame
        BulkVolume.objects.filter(time_frame=time_frame).delete()
        
        # Save all records
        records_created = 0
        for _, row in broker_script_volume.iterrows():
            BulkVolume.objects.create(
                script=row['symbol'],
                buy_broker=row['buyer'],
                quantity=row['quantity'],
                time_frame=time_frame,
                date_range=date_range
            )
            records_created += 1

        self.stdout.write(self.style.SUCCESS(
            f"Saved {records_created} broker-script records for {time_frame} ({date_range})"
        ))
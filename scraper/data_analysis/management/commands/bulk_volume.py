import django
django.setup()
from django.core.management.base import BaseCommand
from django.db import models
from data_analysis.models import FloorsheetData, BulkVolumeTrade
from datetime import datetime, timedelta
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd

class Command(BaseCommand):
    help = 'Process bulk volume trades to find highest quantity per broker with their top script'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Starting bulk volume trade processing...'))
        
        client = Client()
        try:
            # Process data for different time frames
            time_frames = [
                ('Daily', timedelta(days=1)),
                ('Weekly', timedelta(days=5)),
                ('Monthly', timedelta(days=30)),
                ('3 Month', timedelta(days=90)),
                ('6 Month', timedelta(days=180))
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
        
        # IMPROVED APPROACH:
        
        # Step 1: For each broker, find their top script by total trading volume
        # Group by buyer and symbol to get total quantity traded for each pair
        broker_script_volume = df.groupby(['buyer', 'symbol']).agg({'quantity': 'sum'}).reset_index().compute()
        
        # Step 2: Get all unique buyers
        all_buyers = broker_script_volume['buyer'].unique()
        
        # Step 3: For each buyer, find their top script (with highest total quantity)
        final_results = []
        
        for buyer in all_buyers:
            buyer_data = broker_script_volume[broker_script_volume['buyer'] == buyer]
            # Find the script with maximum total quantity for this buyer
            top_script_row = buyer_data.loc[buyer_data['quantity'].idxmax()]
            
            # Once we know the buyer's top script, find the largest single trade for this broker-script pair
            max_single_trade = (
                df[(df['buyer'] == buyer) & (df['symbol'] == top_script_row['symbol'])]
                .compute()['quantity'].max()
            )
            
            final_results.append({
                'buyer': buyer,
                'symbol': top_script_row['symbol'],
                'quantity': max_single_trade
            })
        
        # Convert to DataFrame and sort by quantity
        final_df = pd.DataFrame(final_results).sort_values('quantity', ascending=False)
        
        # Debug output
        self.stdout.write(f"Found {len(final_df)} unique brokers in {time_frame} period")
        
        # Delete existing records for this time frame
        BulkVolumeTrade.objects.filter(time_frame=time_frame).delete()
        
        # Save all records
        records_created = 0
        for _, row in final_df.iterrows():
            BulkVolumeTrade.objects.create(
                script=row['symbol'],
                buy_broker=row['buyer'],
                quantity=row['quantity'],
                time_frame=time_frame,
                date_range=date_range
            )
            records_created += 1

        self.stdout.write(self.style.SUCCESS(
            f"Saved {records_created} broker records for {time_frame} ({date_range})"
        ))
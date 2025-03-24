import django
django.setup()

from django.core.management.base import BaseCommand
from django.db import models
from data_analysis.models import FloorsheetData, WashTrade
from datetime import datetime, timedelta
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd

class Command(BaseCommand):
    help = 'Detects and saves wash trades by time frame with proper date ranges'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Starting wash trade detection...'))
        
        client = Client()
        try:
            latest_date = FloorsheetData.objects.aggregate(models.Max('date'))['date__max']
            if not latest_date:
                self.stdout.write(self.style.WARNING('No data available'))
                return

            # Define all time frames with their parameters
            time_frames = [
                {'name': 'Daily', 'days': 1, 'label': 'Latest Day'},
                {'name': 'Weekly', 'days': 5, 'label': 'Latest 5 Days'},
                {'name': 'Monthly', 'days': 30, 'label': 'Latest 30 Days'},
                {'name': '3 Month', 'days': 90, 'label': 'Latest 90 Days'},
                {'name': '6 Month', 'days': 180, 'label': 'Latest 180 Days'}
            ]

            for tf in time_frames:
                self.process_time_frame(
                    name=tf['name'],
                    label=tf['label'],
                    latest_date=latest_date,
                    days=tf['days']
                )
            
            self.stdout.write(self.style.SUCCESS('\nAll time frames processed successfully'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error: {str(e)}'))
        finally:
            client.close()

    def process_time_frame(self, name, label, latest_date, days):
        """Process and save wash trades for a specific time frame"""
        start_date = latest_date - timedelta(days=days)
        
        # Format the date range display
        date_range = (
            latest_date.strftime('%Y-%m-%d') if name == 'Daily'
            else f"{start_date.strftime('%Y-%m-%d')} to {latest_date.strftime('%Y-%m-%d')}"
        )

        self.stdout.write(f"\nProcessing {label} ({date_range})...")

        # Get data using Dask for efficient processing
        queryset = FloorsheetData.objects.filter(
            date__gte=start_date,
            date__lte=latest_date
        ).values('symbol', 'buyer', 'seller', 'quantity')

        df = dd.from_pandas(pd.DataFrame(list(queryset)), npartitions=10)
        
        # Filter wash trades (same buyer/seller with quantity > 1000)
        wash_trades = df[
            (df['buyer'] == df['seller']) & 
            (df['quantity'] > 1000)
        ]

        if len(wash_trades.index) == 0:
            self.stdout.write(self.style.WARNING('No wash trades found'))
            WashTrade.objects.filter(time_frame=name).delete()
            return

        # Process results
        result = (
            wash_trades.groupby(['symbol', 'buyer'])
            .agg({'quantity': 'sum'})
            .reset_index()
            .sort_values('quantity', ascending=False)
            .compute()
        )

        # Clear existing records for this time frame
        WashTrade.objects.filter(time_frame=name).delete()
        
        # Save new records
        records_created = 0
        for _, row in result.iterrows():
            WashTrade.objects.create(
                script=row['symbol'],
                quantity=row['quantity'],
                time_frame=name,
                date_range=date_range,
                buyer_seller=row['buyer']
            )
            records_created += 1

        total_volume = result['quantity'].sum()
        self.stdout.write(self.style.SUCCESS(
            f"Saved {records_created} wash trades (Total volume: {total_volume:,} shares)"
        ))
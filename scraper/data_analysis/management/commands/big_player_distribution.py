import django
import json
from django.core.management.base import BaseCommand
from django.db import models
from data_analysis.models import FloorsheetData, BigPlayerDistribution
from datetime import datetime, timedelta
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd

class Command(BaseCommand):
    help = 'Detects big player distribution patterns (one seller, multiple buyers)'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Starting big player distribution detection...'))
        
        client = Client()
        try:
            latest_date = FloorsheetData.objects.aggregate(models.Max('date'))['date__max']
            if not latest_date:
                self.stdout.write(self.style.WARNING('No data available'))
                return

            # Process all time frames
            time_frames = [
                {'name': 'Daily', 'days': 1, 'label': 'Latest Day'},
                {'name': 'Weekly', 'days': 4, 'label': 'Latest 5 Days'},
                {'name': 'Monthly', 'days': 29, 'label': 'Latest 30 Days'},
                {'name': '3 Month', 'days': 89, 'label': 'Latest 90 Days'},
                {'name': '6 Month', 'days': 179, 'label': 'Latest 180 Days'}
            ]

            for tf in time_frames:
                self.process_time_frame(
                    name=tf['name'],
                    label=tf['label'],
                    latest_date=latest_date,
                    days=tf['days']
                )
            
            self.stdout.write(self.style.SUCCESS('\nProcessing completed'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error: {str(e)}'))
        finally:
            client.close()

    def process_time_frame(self, name, label, latest_date, days):
        """Process distribution patterns for a specific time frame"""
        start_date = latest_date - timedelta(days=days)
        
        # Format the date range display
        date_range = (
            latest_date.strftime('%Y-%m-%d') if name == 'Daily'
            else f"{start_date.strftime('%Y-%m-%d')} to {latest_date.strftime('%Y-%m-%d')}"
        )

        self.stdout.write(f"\nProcessing {label} ({date_range})...")

        # Get data using Dask
        queryset = FloorsheetData.objects.filter(
            date__gte=start_date,
            date__lte=latest_date
        ).values('symbol', 'buyer', 'seller', 'quantity', 'date')

        # Convert to DataFrame
        df = dd.from_pandas(pd.DataFrame(list(queryset)), npartitions=10)
        
        # Step 1: Calculate the sum of quantity for each symbol-seller pair
        quantity_sum = df.groupby(['symbol', 'seller']).quantity.sum().reset_index()
        
        # Step 2: Bring everything to Pandas for more complex operations
        pandas_df = df.compute()
        
        # Group by symbol and seller to get all unique buyers
        buyers_groups = pandas_df.groupby(['symbol', 'seller'])['buyer'].apply(lambda x: list(x.unique()))
        buyers_df = buyers_groups.reset_index()
        
        # Get the latest date for each symbol-seller pair
        latest_dates = pandas_df.groupby(['symbol', 'seller'])['date'].max().reset_index()
        
        # Merge all the information together
        merged_df = pd.merge(quantity_sum.compute(), buyers_df, on=['symbol', 'seller'])
        merged_df = pd.merge(merged_df, latest_dates, on=['symbol', 'seller'])
        
        # Filter for cases with multiple buyers
        result = merged_df[merged_df['buyer'].apply(len) > 1]
        
        # Sort by quantity
        result = result.sort_values('quantity', ascending=False)
        
        if len(result) == 0:
            self.stdout.write(self.style.WARNING('No distribution patterns found'))
            BigPlayerDistribution.objects.filter(time_frame=name).delete()
            return

        # Update database
        BigPlayerDistribution.objects.filter(time_frame=name).delete()
        
        records_created = 0
        for _, row in result.iterrows():
            BigPlayerDistribution.objects.create(
                script=row['symbol'],
                quantity=row['quantity'],
                buying_brokers=json.dumps(row['buyer']),  # Correct field name
                selling_broker=row['seller'],  # Correct field name
                time_frame=name,
                date_range=date_range
            )
            records_created += 1

        total_volume = result['quantity'].sum()
        self.stdout.write(self.style.SUCCESS(
            f"Found {records_created} distribution patterns (Total volume: {total_volume:,} shares)"
        ))
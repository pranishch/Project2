import django
django.setup()
import json
from django.core.management.base import BaseCommand
from django.db import models
from data_analysis.models import FloorsheetData, BigPlayerAccumulation
from datetime import datetime, timedelta
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd

class Command(BaseCommand):
    help = 'Detects big player accumulation patterns (one buyer, multiple sellers)'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Starting big player accumulation detection...'))
        
        client = Client()
        try:
            latest_date = FloorsheetData.objects.aggregate(models.Max('date'))['date__max']
            if not latest_date:
                self.stdout.write(self.style.WARNING('No data available'))
                return

            # Process all time frames
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
            
            self.stdout.write(self.style.SUCCESS('\nProcessing completed'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error: {str(e)}'))
        finally:
            client.close()

    def process_time_frame(self, name, label, latest_date, days):
        """Process accumulation patterns for a specific time frame"""
        start_date = latest_date - timedelta(days=days)
        
        # Create descriptive date range
        if name == 'Daily':
            date_range = f"As of {latest_date.strftime('%Y-%m-%d')}"
        else:
            date_range = f"{days} days up to {latest_date.strftime('%Y-%m-%d')}"

        self.stdout.write(f"\nProcessing {label} ({date_range})")

        # Get data using Dask
        queryset = FloorsheetData.objects.filter(
            date__gte=start_date,
            date__lte=latest_date
        ).values('symbol', 'buyer', 'seller', 'quantity')

        df = dd.from_pandas(pd.DataFrame(list(queryset)), npartitions=10)
        
        # Find accumulation patterns (one buyer, multiple sellers)
        result = (
            df.groupby(['symbol', 'buyer'])
            .agg({
                'quantity': 'sum',
                'seller': lambda x: list(x.unique()),
                'date': 'max'
            })
            .reset_index()
            .rename(columns={'seller': 'sellers'})
        )

        # Filter for cases with multiple sellers
        result = result[result['sellers'].map(len) > 1]
        
        if len(result.index) == 0:
            self.stdout.write(self.style.WARNING('No accumulation patterns found'))
            BigPlayerAccumulation.objects.filter(time_frame=name).delete()
            return

        # Compute and sort results
        result = (
            result.sort_values('quantity', ascending=False)
            .compute()
        )

        # Update database
        BigPlayerAccumulation.objects.filter(time_frame=name).delete()
        
        records_created = 0
        for _, row in result.iterrows():
            BigPlayerAccumulation.objects.create(
                script=row['symbol'],
                quantity=row['quantity'],
                buying_broker=row['buyer'],
                selling_brokers=json.dumps(row['sellers']),
                time_frame=name,
                date_range=date_range
            )
            records_created += 1

        total_volume = result['quantity'].sum()
        self.stdout.write(self.style.SUCCESS(
            f"Found {records_created} accumulation patterns (Total volume: {total_volume:,} shares)"
        ))
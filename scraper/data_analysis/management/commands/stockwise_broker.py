from django.core.management.base import BaseCommand
from django.db import transaction
from data_analysis.models import FloorsheetData, StockwiseBroker
import pandas as pd
import dask.dataframe as dd
from datetime import datetime, timedelta
from dask.distributed import Client

class Command(BaseCommand):
    help = 'Analyzes broker volumes for all time frames and refreshes database'

    def add_arguments(self, parser):
        parser.add_argument(
            '--stock',
            type=str,
            default=None,
            help='Process specific stock only'
        )
        parser.add_argument(
            '--min-percent',
            type=float,
            default=0.1,
            help='Minimum percentage volume to include (default: 0.1%%)'
        )

    def handle(self, *args, **options):
        self.specific_stock = options['stock']
        self.min_percent = options['min_percent']
        
        with Client() as client:
            try:
                if self.specific_stock:
                    self.stdout.write(f"Refreshing {self.specific_stock} data for all time frames")
                    self.process_stock(self.specific_stock)
                else:
                    self.stdout.write("Refreshing all stocks for all time frames")
                    self.process_all_stocks()
                    
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error: {str(e)}"))
                raise

    def get_time_ranges(self):
        """Returns all time frames with their date ranges"""
        latest_date = FloorsheetData.objects.latest('date').date
        return {
            'daily': (latest_date, latest_date),
            'weekly': (latest_date - timedelta(days=6), latest_date),
            'monthly': (latest_date - timedelta(days=29), latest_date),
            '3month': (latest_date - timedelta(days=89), latest_date),
            '6month': (latest_date - timedelta(days=179), latest_date)
        }

    def process_all_stocks(self):
        """Process all stocks"""
        stocks = FloorsheetData.objects.values_list('symbol', flat=True).distinct()
        for stock in stocks:
            self.process_stock(stock)

    def process_stock(self, stock_name):
        """Process single stock for all time frames"""
        try:
            # Get all data for the stock once
            queryset = FloorsheetData.objects.filter(symbol=stock_name)
            if not queryset.exists():
                self.stdout.write(self.style.WARNING(f"Skipping {stock_name} - no data"))
                return

            # Convert to Dask DataFrame
            df = pd.DataFrame(list(queryset.values('date', 'quantity', 'buyer', 'seller')))
            ddf = dd.from_pandas(df, npartitions=4)
            ddf['date'] = dd.to_datetime(ddf['date']).dt.date

            # Process each time frame
            for time_frame, (start_date, end_date) in self.get_time_ranges().items():
                self.refresh_time_frame(stock_name, ddf, time_frame, start_date, end_date)
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Failed on {stock_name}: {str(e)}"))
            raise

    def refresh_time_frame(self, stock_name, ddf, time_frame, start_date, end_date):
        """Clear and refresh data for specific time frame"""
        with transaction.atomic():
            # First delete existing records for this stock+timeframe
            deleted_count, _ = StockwiseBroker.objects.filter(
                stock_name=stock_name,
                time_frame=time_frame
            ).delete()
            
            self.stdout.write(f"Cleared {deleted_count} old records for {stock_name} ({time_frame})")

            # Filter data for time frame
            filtered = ddf[(ddf['date'] >= start_date) & (ddf['date'] <= end_date)]
            
            # Calculate volumes
            buyers = filtered.groupby('buyer')['quantity'].sum().compute()
            sellers = filtered.groupby('seller')['quantity'].sum().compute()
            
            # Combine and filter
            brokers = pd.concat([buyers, sellers], axis=1, keys=['buy', 'sell']).fillna(0)
            brokers['total'] = brokers['buy'] + brokers['sell']
            grand_total = brokers['total'].sum()
            
            if grand_total == 0:
                self.stdout.write(f"No volume for {stock_name} ({time_frame})")
                return

            # Calculate percentages and filter
            brokers['percent'] = (brokers['total'] / grand_total) * 100
            brokers = brokers[brokers['percent'] >= self.min_percent].sort_values('total', ascending=False)

            # Prepare records
            date_range = (
                end_date.strftime("%Y-%m-%d") if time_frame == 'daily'
                else f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            )

            records = [
                StockwiseBroker(
                    stock_name=stock_name,
                    broker_id=broker_id,
                    volume=int(row['total']),
                    percent_volume=round(row['percent'], 2),
                    date_range=date_range,
                    time_frame=time_frame
                )
                for broker_id, row in brokers.iterrows()
            ]

            # Bulk create new records
            StockwiseBroker.objects.bulk_create(records)
            self.stdout.write(self.style.SUCCESS(
                f"Saved {len(records)} brokers for {stock_name} ({time_frame})"
            ))

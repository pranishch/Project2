# management/commands/distribution.py
from django.core.management.base import BaseCommand
from django.db import transaction
import dask.dataframe as dd
from data_analysis.models import FloorsheetData, DistributionAnalysis, LargeSellers  # Adjust import based on your app name
import pandas as pd
from datetime import datetime

class Command(BaseCommand):
    help = 'Process distribution analysis using Dask and save results to database'

    def handle(self, *args, **options):
        try:
            # Clear existing data
            with transaction.atomic():
                DistributionAnalysis.objects.all().delete()
                LargeSellers.objects.all().delete()

            # Read data from database using Dask
            # First get pandas dataframe then convert to dask
            floorsheet_qs = FloorsheetData.objects.values(
                'symbol', 'rate', 'quantity', 'date'
            )
            pd_df = pd.DataFrame.from_records(floorsheet_qs)
            df = dd.from_pandas(pd_df, npartitions=4)  # Adjust partitions based on data size

            # Rename columns to match expected format
            df = df.rename(columns={
                'rate': 'Price',
                'quantity': 'Volume',
                'date': 'Date'
            })

            # Daily Data calculation
            daily_data = (df.groupby(['Date'])
                         .agg({
                             'Price': 'mean',
                             'Volume': 'sum'
                         })
                         .rename(columns={
                             'Price': 'Avg_Price',
                             'Volume': 'Total_Volume'
                         })
                         .reset_index()
                         .compute())  # Compute to get pandas DataFrame

            # Spot Distribution Signals (volume > 500)
            large_trades = df[df['Volume'] > 500].compute()
            
            # Identify bearish signals (large selling at stable or rising prices)
            spot_signals = []
            for date in large_trades['Date'].unique():
                daily_subset = daily_data[daily_data['Date'] == date]
                prev_day = daily_data[daily_data['Date'] < date].tail(1)
                
                if not prev_day.empty and not daily_subset.empty:
                    current_price = daily_subset['Avg_Price'].iloc[0]
                    prev_price = prev_day['Avg_Price'].iloc[0]
                    if current_price >= prev_price:  # Stable or rising prices
                        spot_signals.append(date)

            # Prepare Large Sellers data
            large_sellers_data = large_trades.rename(columns={
                'Date': 'Timestamp'
            })
            # Add timestamp with dummy time (11:00:00) as per example
            large_sellers_data['Timestamp'] = pd.to_datetime(
                large_sellers_data['Timestamp']
            ).dt.strftime('%Y-%m-%d 11:00:00')

            # Save to database
            with transaction.atomic():
                # Save Daily Data
                daily_instances = [
                    DistributionAnalysis(
                        timestamp=row['Date'],
                        avg_price=float(row['Avg_Price']),
                        total_volume=float(row['Total_Volume'])
                    )
                    for _, row in daily_data.iterrows()
                ]
                DistributionAnalysis.objects.bulk_create(daily_instances)

                # Save Large Sellers
                large_sellers_instances = [
                    LargeSellers(
                        symbol=row['symbol'],
                        price=float(row['Price']),
                        volume=float(row['Volume']),
                        timestamp=row['Timestamp']
                    )
                    for _, row in large_sellers_data.iterrows()
                ]
                LargeSellers.objects.bulk_create(large_sellers_instances)

            # Log success
            self.stdout.write(self.style.SUCCESS(
                f'Successfully processed distribution analysis. '
                f'Spot signals detected on: {spot_signals}'
            ))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error processing distribution: {str(e)}'))



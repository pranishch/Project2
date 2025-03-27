import dask.dataframe as dd
from django.core.management.base import BaseCommand
from django.db.models import Max
import pandas as pd
from data_analysis.models import FloorsheetData, WallBreakTracking
from datetime import datetime

class Command(BaseCommand):
    help = 'Track wall breaks by comparing price to resistance levels using Dask for big data processing'

    def handle(self, *args, **options):
        self.stdout.write("Starting wall break tracking process...")
        
        try:
            # Get the latest date from FloorsheetData
            latest_date = FloorsheetData.objects.aggregate(Max('date'))['date__max']
            if not latest_date:
                self.stdout.write(self.style.ERROR('No data found in FloorsheetData'))
                return

            # Convert FloorsheetData to Dask DataFrame for processing
            self.stdout.write("Loading data into Dask DataFrame...")
            
            # Get all data for the latest date
            qs = FloorsheetData.objects.filter(date=latest_date).values(
                'symbol', 'rate', 'date'
            )
            
            # Convert to Dask DataFrame
            df = dd.from_pandas(pd.DataFrame(list(qs)), npartitions=10)
            
            # Calculate average price (resistance level) per symbol
            self.stdout.write("Calculating resistance levels...")
            resistance_df = df.groupby('symbol')['rate'].mean().reset_index()
            resistance_df = resistance_df.compute()  # Compute the result
            
            # Get current price (latest rate) per symbol
            current_price_df = df.groupby('symbol')['rate'].last().reset_index()
            current_price_df = current_price_df.compute()
            
            # Merge resistance and current price data
            merged_df = resistance_df.merge(
                current_price_df, 
                on='symbol', 
                suffixes=('_resistance', '_current')
            )
            
            # Determine remarks based on price vs resistance
            def get_remark(row):
                if row['rate_current'] > row['rate_resistance']:
                    return "Runs above"
                else:
                    return "Chances of down price"
            
            merged_df['remarks'] = merged_df.apply(get_remark, axis=1)
            
            # Rename columns for database model
            merged_df = merged_df.rename(columns={
                'symbol': 'script_name',
                'rate_resistance': 'resistance',
                'rate_current': 'price',
            })
            
            # Add date column
            merged_df['date'] = latest_date
            
            # Save to WallBreakTracking model
            self.stdout.write("Saving results to database...")
            records = merged_df.to_dict('records')
            
            # Bulk create records
            WallBreakTracking.objects.bulk_create([
                WallBreakTracking(**record) for record in records
            ])
            
            self.stdout.write(self.style.SUCCESS(
                f"Successfully processed {len(records)} wall break records for date {latest_date}"
            ))
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error processing wall breaks: {str(e)}'))
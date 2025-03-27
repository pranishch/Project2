import django
import json
from django.core.management.base import BaseCommand
from django.db import models
from data_analysis.models import FloorsheetData, BrokerWiseDistribution
from datetime import datetime, timedelta
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd

class Command(BaseCommand):
    help = 'Detects broker selling quantities across multiple time frames'

    def handle(self, *args, **options):
        """
        Main handler for broker selling quantities detection
        """
        self.stdout.write(self.style.SUCCESS('Starting broker selling quantities detection...'))
        
        client = Client()
        try:
            # Get the latest date in the dataset
            latest_date = FloorsheetData.objects.aggregate(models.Max('date'))['date__max']
            
            # Define time frames
            time_frames = [
                {'name': 'Daily', 'days': 1},
                {'name': 'Weekly', 'days': 7},
                {'name': 'Monthly', 'days': 30},
                {'name': '3 Month', 'days': 90},
                {'name': '6 Month', 'days': 180}
            ]

            # Process each time frame
            for tf in time_frames:
                # Calculate start date
                start_date = latest_date - timedelta(days=tf['days'])
                
                # Format date range for display and storage
                date_range = (
                    latest_date.strftime('%Y-%m-%d') if tf['name'] == 'Daily'
                    else f"{start_date.strftime('%Y-%m-%d')} to {latest_date.strftime('%Y-%m-%d')}"
                )

                # Fetch ONLY selling data using Dask
                queryset = FloorsheetData.objects.filter(
                    date__gte=start_date,
                    date__lte=latest_date
                ).values('symbol', 'seller', 'quantity')

                # Convert to Dask DataFrame
                df = dd.from_pandas(pd.DataFrame(list(queryset)), npartitions=10)
                
                # Compute broker and script-wise selling quantities
                # Strictly focusing on seller's quantities
                selling_quantities = df.groupby(['seller', 'symbol']).agg({
                    'quantity': 'sum'
                }).reset_index()
                
                # Convert to Pandas for final processing
                result_df = selling_quantities.compute()
                
                # Sort by quantity in descending order
                result_df = result_df.sort_values('quantity', ascending=False)

                # Remove existing records for this time frame
                BrokerWiseDistribution.objects.filter(
                    time_frame=tf['name']
                ).delete()

                # Save results to database
                records_created = 0
                for _, row in result_df.iterrows():
                    BrokerWiseDistribution.objects.create(
                        broker=row['seller'],
                        script=row['symbol'],
                        selling_quantity=row['quantity'],
                        date_range=date_range,
                        time_frame=tf['name']
                    )
                    records_created += 1

                # Output summary for this time frame
                total_volume = result_df['quantity'].sum()
                self.stdout.write(self.style.SUCCESS(
                    f"{tf['name']} - Found {records_created} selling patterns "
                    f"(Total selling volume: {total_volume:,} shares)"
                ))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error: {str(e)}'))
        finally:
            client.close()
from django.core.management.base import BaseCommand
from django.db import transaction
from data_analysis.models import FloorsheetData, VolumeSpikeDetection
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd
from datetime import datetime, timedelta

class Command(BaseCommand):
    help = 'Detects volume spikes matching the exact report format'

    def add_arguments(self, parser):
        parser.add_argument(
            '--duration',
            type=str,
            choices=['week', 'month'],
            default='week',
            help='Duration: week (5 days) or month (20 days)'
        )

    def handle(self, *args, **options):
        duration = options['duration']
        
        # Start Dask client
        client = Client()
        
        try:
            # Date range calculation
            end_date = datetime.now().date()
            if duration == 'week':
                start_date = end_date - timedelta(days=5)
                self.stdout.write("Processing last 5 trading days...")
            else:
                start_date = end_date - timedelta(days=20)
                self.stdout.write("Processing last 20 trading days...")

            # Load data
            queryset = FloorsheetData.objects.filter(
                date__gte=start_date,
                date__lte=end_date
            ).values('symbol', 'quantity', 'date')
            
            pdf = pd.DataFrame.from_records(queryset)
            if pdf.empty:
                self.stdout.write(self.style.WARNING("No data found."))
                return
                
            ddf = dd.from_pandas(pdf, npartitions=4)
            
            # Calculate volumes
            daily_volume = ddf.groupby(['date', 'symbol'])['quantity'].sum().reset_index()
            avg_volume = daily_volume.groupby('symbol')['quantity'].mean().reset_index()
            avg_volume = avg_volume.rename(columns={'quantity': 'avg_quantity'})
            
            # Merge and compute
            merged = dd.merge(daily_volume, avg_volume, on='symbol')
            merged['volume_percent'] = (merged['quantity'] / merged['avg_quantity']) * 100
            result = merged.compute()
            
            # Filter spikes (>5% above average)
            spikes = result[result['volume_percent'] >= 105]
            top_spikes = spikes.nlargest(20, 'volume_percent')
            
            # Clear and save
            with transaction.atomic():
                VolumeSpikeDetection.objects.all().delete()
                records = [
                    VolumeSpikeDetection(
                        symbol=row['symbol'],
                        volume=row['quantity'],
                        avg_volume_percent=row['volume_percent'],
                        date=row['date']
                    )
                    for _, row in top_spikes.iterrows()
                ]
                VolumeSpikeDetection.objects.bulk_create(records)
            
            # Print report-matched output
            if len(top_spikes) > 0:
                self.stdout.write("\nVolume(qty)\tAvg Volume%\tDate")
                self.stdout.write("----------------------------------")
                for _, row in top_spikes.iterrows():
                    self.stdout.write(
                        f"{int(row['quantity'])}\t\t"
                        f"{row['volume_percent']:.0f}%\t\t"
                        f"{row['date'].strftime('%m/%d')}"  # MM/DD format
                    )
            else:
                self.stdout.write("No volume spikes detected.")
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error: {str(e)}"))
        finally:
            client.close()
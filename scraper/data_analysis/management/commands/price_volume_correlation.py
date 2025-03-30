from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
import dask.dataframe as dd
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dask.distributed import Client
from data_analysis.models import FloorsheetData, PriceVolumeCorrelation

class Command(BaseCommand):
    help = 'Analyzes price-volume correlation for the latest trading day'

    def add_arguments(self, parser):
        parser.add_argument(
            '--volume-threshold',
            type=float,
            default=1.5,
            help='Volume multiplier to consider as significant (default: 1.5x avg)'
        )

    def handle(self, *args, **options):
        volume_threshold = options['volume_threshold']
        
        try:
            # Get the latest date available in the database
            latest_date = FloorsheetData.objects.latest('date').date
            self.stdout.write(f"Analyzing price-volume correlation for {latest_date}...")
            
            with Client() as client:
                # Load data for the latest date and previous 2 days for averages
                self.stdout.write("Loading data...")
                df = self.load_data_for_analysis(latest_date)
                
                if df is None or len(df) == 0:
                    self.stdout.write(self.style.WARNING(f"No data found for {latest_date}"))
                    return
                
                # Calculate correlations
                self.stdout.write("Calculating correlations...")
                correlations = self.calculate_latest_correlations(df, latest_date, volume_threshold)
                
                # Save results
                with transaction.atomic():
                    PriceVolumeCorrelation.objects.all().delete()
                    
                    saved = self.save_correlations(correlations)
                    self.stdout.write(self.style.SUCCESS(
                        f"Found {saved} price-volume correlations for {latest_date}"
                    ))
                    
        except FloorsheetData.DoesNotExist:
            self.stdout.write(self.style.ERROR("No floorsheet data available"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Processing error: {str(e)}"))

    def load_data_for_analysis(self, latest_date):
        """Load data for latest date and previous 2 days"""
        # Convert to pandas datetime for consistency
        latest_date = pd.to_datetime(latest_date)
        date_range = [latest_date - pd.Timedelta(days=i) for i in range(3)]
        
        # Convert date_range to datetime.date for Django query
        django_date_range = [d.date() for d in date_range]
        
        ddf = dd.from_pandas(
            pd.DataFrame(
                list(FloorsheetData.objects.filter(
                    date__in=django_date_range,
                    rate__isnull=False,
                    quantity__isnull=False
                ).values(
                    'symbol', 'rate', 'quantity', 'date'
                )),
                columns=['symbol', 'rate', 'quantity', 'date']
            ),
            npartitions=2
        )
        ddf['date'] = dd.to_datetime(ddf['date'])
        return ddf

    def calculate_latest_correlations(self, ddf, latest_date, volume_threshold):
        """Calculate price-volume correlations for latest date"""
        # Convert to pandas for the small date range
        df = ddf.compute()
        
        # Ensure consistent datetime type for comparison
        latest_date = pd.to_datetime(latest_date)
        
        # Calculate 3-day averages (current day + 2 previous)
        avg_data = df.groupby(['symbol']).apply(
            lambda x: pd.Series({
                'avg_price': x[x['date'] < latest_date]['rate'].mean(),
                'avg_volume': x[x['date'] < latest_date]['quantity'].sum()
            })
        ).reset_index()
        
        # Get latest day metrics
        latest_stats = df[df['date'] == latest_date].groupby('symbol').agg(
            price=('rate', 'mean'),
            volume=('quantity', 'sum')
        ).reset_index()
        
        # Merge with averages
        merged = pd.merge(latest_stats, avg_data, on='symbol', how='left')
        
        # Calculate metrics
        merged['price_change_pct'] = (
            (merged['price'] - merged['avg_price']) / 
            merged['avg_price'].replace(0, np.nan) * 100
        ).fillna(0)
        
        merged['volume_ratio'] = (
            merged['volume'] / merged['avg_volume'].replace(0, np.nan)
        ).fillna(1)
        
        # Classify movements
        conditions = [
            (merged['price_change_pct'] > 0) & (merged['volume_ratio'] >= volume_threshold),
            (merged['price_change_pct'] > 0) & (merged['volume_ratio'] < volume_threshold),
            (merged['price_change_pct'] < 0) & (merged['volume_ratio'] >= volume_threshold),
            (merged['price_change_pct'] < 0) & (merged['volume_ratio'] < volume_threshold)
        ]
        choices = ['strong_rise', 'weak_rise', 'strong_fall', 'weak_fall']
        merged['movement_type'] = np.select(conditions, choices, default='neutral')
        
        # Add date column (convert back to datetime.date for Django)
        merged['date'] = latest_date.date()
        
        return merged[['symbol', 'date', 'movement_type', 
                      'price_change_pct', 'volume_ratio',
                      'price', 'volume']]

    def save_correlations(self, correlations_df):
        """Save correlations to database"""
        if correlations_df is None or len(correlations_df) == 0:
            return 0
            
        records = [
            PriceVolumeCorrelation(
                symbol=row['symbol'],
                date=row['date'],
                movement_type=row['movement_type'],
                price_change=row['price_change_pct'],
                volume_ratio=row['volume_ratio'],
                price=row['price'],
                volume=row['volume']
            )
            for _, row in correlations_df.iterrows()
        ]
        PriceVolumeCorrelation.objects.bulk_create(records)
        return len(records)
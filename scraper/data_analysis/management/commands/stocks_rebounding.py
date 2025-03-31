from django.core.management.base import BaseCommand
from data_analysis.models import FloorsheetData, StockReboundReport
from django.db import transaction
from datetime import timedelta
import dask.dataframe as dd
import pandas as pd

class Command(BaseCommand):
    help = 'Identifies stocks rebounding after 3+ consecutive days of drops'
    
    # Configuration
    MIN_DROP_DAYS = 3
    REBOUND_THRESHOLD = 1.5  # Minimum percentage gain to consider it a rebound
    MIN_VOLUME_CHANGE = 20   # Minimum volume increase percentage
    
    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Identifying rebounding stocks...'))
        
        # Clear existing data
        StockReboundReport.objects.all().delete()
        
        # Get data as pandas DataFrame first
        queryset = FloorsheetData.objects.all().values('symbol', 'date', 'rate', 'quantity')
        pdf = pd.DataFrame(list(queryset))
        
        # Convert to Dask DataFrame
        df = dd.from_pandas(pdf, npartitions=10)
        
        # Preprocessing - convert date and sort
        df['date'] = dd.to_datetime(df['date'])
        df = df.sort_values(['symbol', 'date'])
        
        # Calculate daily stats as pandas operations first
        with pd.option_context('mode.chained_assignment', None):
            # Compute to pandas for these operations
            daily_stats = df.compute()
            
            # Calculate percentage changes
            daily_stats['pct_change'] = daily_stats.groupby('symbol')['rate'].pct_change() * 100
            daily_stats['volume_change'] = daily_stats.groupby('symbol')['quantity'].pct_change() * 100
            
            # Identify consecutive drops and rebounds
            rebound_candidates = []
            
            for symbol, group in daily_stats.groupby('symbol'):
                group = group.sort_values('date')
                group['is_drop'] = group['pct_change'] < 0
                group['drop_streak'] = group['is_drop'].astype(int).groupby(
                    (~group['is_drop']).cumsum()).cumsum()
                
                # Find rebound opportunities
                for i in range(1, len(group)):
                    current = group.iloc[i]
                    previous = group.iloc[i-1]
                    
                    if (previous['drop_streak'] >= self.MIN_DROP_DAYS and 
                        current['pct_change'] >= self.REBOUND_THRESHOLD and
                        current['volume_change'] >= self.MIN_VOLUME_CHANGE):
                        
                        # Calculate metrics for the drop period
                        drop_start_idx = i - previous['drop_streak']
                        drop_period = group.iloc[drop_start_idx:i+1]
                        total_drop = ((current['rate'] - drop_period.iloc[0]['rate']) / 
                                    drop_period.iloc[0]['rate']) * 100
                        
                        rebound_candidates.append({
                            'symbol': symbol,
                            'rebound_date': current['date'].date(),
                            'consecutive_drops': previous['drop_streak'],
                            'drop_percentage': total_drop,
                            'rebound_percentage': current['pct_change'],
                            'volume_change': current['volume_change']
                        })
        
        # Save to database
        if rebound_candidates:
            report_entries = [
                StockReboundReport(
                    symbol=row['symbol'],
                    rebound_date=row['rebound_date'],
                    consecutive_drops=row['consecutive_drops'],
                    drop_percentage=row['drop_percentage'],
                    rebound_percentage=row['rebound_percentage'],
                    volume_change=row['volume_change']
                )
                for row in rebound_candidates
            ]
            
            with transaction.atomic():
                StockReboundReport.objects.bulk_create(report_entries)
            
            self.stdout.write(self.style.SUCCESS(
                f"Found {len(report_entries)} stocks rebounding after {self.MIN_DROP_DAYS}+ consecutive drops"
            ))
        else:
            self.stdout.write(self.style.WARNING("No rebounding stocks found"))
from django.core.management.base import BaseCommand
from django.db import transaction, connection
from dask import dataframe as dd
from dask.distributed import Client
from ...models import FloorsheetData, SingleDayHighVolume
import datetime
import pandas as pd
import sys
import numpy as np


class Command(BaseCommand):
    help = 'Identifies stocks with trading volume 3x+ higher than their weekly average'

    def add_arguments(self, parser):
        parser.add_argument(
            '--multiplier',
            type=float,
            default=3.0,
            help='Initial volume multiplier threshold (default: 3.0)'
        )
        parser.add_argument(
            '--min-multiplier',
            type=float,
            default=1.5,
            help='Minimum multiplier to try if no alerts found (default: 1.5)'
        )
        parser.add_argument(
            '--lookback-days',
            type=int,
            default=7,
            help='Number of days to calculate average volume (default: 7)'
        )
        parser.add_argument(
            '--current-date',
            type=str,
            default=None,
            help='Override current date (YYYY-MM-DD)'
        )

    def handle(self, *args, **options):
        self.stdout.write("Starting single day high volume detection...")
        
        initial_multiplier = options['multiplier']
        min_multiplier = options['min_multiplier']
        lookback_days = options['lookback_days']
        effective_multiplier = initial_multiplier
        
        # Calculate date ranges with proper type conversion
        if options['current_date']:
            current_date = datetime.datetime.strptime(options['current_date'], '%Y-%m-%d').date()
        else:
            # Use the most recent date in the database if today has no data
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT MAX(date) FROM {FloorsheetData._meta.db_table}")
                max_date = cursor.fetchone()[0]
                if max_date:
                    if isinstance(max_date, str):
                        current_date = datetime.datetime.strptime(max_date, '%Y-%m-%d').date()
                    else:
                        current_date = max_date
                else:
                    current_date = datetime.date.today()
        
        start_date = current_date - datetime.timedelta(days=lookback_days)
        
        self.stdout.write(f"Analyzing data from {start_date} to {current_date}")
        
        client = None
        try:
            client = Client(processes=False)
            
            with connection.cursor() as cursor:
                # Get the most recent lookback_days of data (including current date)
                historical_query = f"""
                    SELECT 
                        symbol,
                        date,
                        SUM(quantity) as daily_volume
                    FROM {FloorsheetData._meta.db_table}
                    WHERE date BETWEEN %s AND %s
                    GROUP BY symbol, date
                    ORDER BY symbol, date
                """
                cursor.execute(historical_query, [start_date, current_date])
                all_data = cursor.fetchall()
                
                if not all_data:
                    self.stdout.write(
                        self.style.ERROR("No data found in the specified date range")
                    )
                    return
                
                # Create DataFrame
                columns = ['symbol', 'date', 'daily_volume']
                df = pd.DataFrame(all_data, columns=columns)
                df['date'] = pd.to_datetime(df['date']).dt.date
                
                # Separate current day data
                current_mask = df['date'] == current_date
                current_df = df[current_mask].copy()
                hist_df = df[~current_mask].copy()
                
                if current_df.empty:
                    self.stdout.write(
                        self.style.WARNING(f"No data found for current date: {current_date}")
                    )
                    return
                
                # Calculate averages
                avg_volume = (hist_df.groupby('symbol')['daily_volume']
                             .mean()
                             .reset_index()
                             .rename(columns={'daily_volume': 'avg_volume'}))
                
                # Merge and calculate ratios
                merged = pd.merge(current_df, avg_volume, on='symbol', how='left')
                merged['volume_ratio'] = merged['daily_volume'] / merged['avg_volume']
                
                # Handle cases where avg_volume is 0 or NaN
                merged['volume_ratio'] = merged['volume_ratio'].replace([np.inf, -np.inf], np.nan)
                
                # Try progressively lower thresholds if no alerts found
                test_multipliers = sorted(
                    [initial_multiplier, 2.5, 2.0, 1.5],
                    reverse=True
                )
                test_multipliers = [m for m in test_multipliers if m >= min_multiplier]
                
                alerts = pd.DataFrame()
                for test_mult in test_multipliers:
                    temp_alerts = merged[merged['volume_ratio'] >= test_mult].copy()
                    if not temp_alerts.empty:
                        alerts = temp_alerts
                        effective_multiplier = test_mult
                        if test_mult < initial_multiplier:
                            self.stdout.write(
                                self.style.NOTICE(
                                    f"No alerts at ≥{initial_multiplier}x, "
                                    f"using ≥{test_mult}x threshold instead"
                                )
                            )
                        break
                
                if alerts.empty:
                    self.stdout.write(
                        self.style.WARNING(
                            f"No stocks found with volume ≥{min_multiplier}x average\n"
                            f"Highest ratio found: {merged['volume_ratio'].max():.2f}x"
                        )
                    )
                    return
                
                # Prepare records
                records_to_save = [
                    SingleDayHighVolume(
                        symbol=row['symbol'],
                        alert_date=row['date'],
                        volume=row['daily_volume'],
                        avg_volume=row['avg_volume'],
                        volume_ratio=row['volume_ratio'],
                        multiplier_threshold=effective_multiplier
                    )
                    for _, row in alerts.iterrows()
                ]
                
                # Save to database
                with transaction.atomic():
                    SingleDayHighVolume.objects.all().delete()
                    SingleDayHighVolume.objects.bulk_create(records_to_save)
                
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Saved {len(records_to_save)} high volume alerts (≥{effective_multiplier}x)\n"
                        "Top 5 by volume ratio:\n" +
                        alerts.nlargest(5, 'volume_ratio')[['symbol', 'daily_volume', 'avg_volume', 'volume_ratio']]
                        .to_string(index=False)
                    )
                )
                
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"Error: {str(e)}\n"
                               "Common fixes:\n"
                               "1. Check if database has data for selected dates\n"
                               "2. Verify model field names match script\n"
                               "3. Check timezone settings if dates appear wrong")
            )
            sys.exit(1)
        finally:
            if client:
                client.close()
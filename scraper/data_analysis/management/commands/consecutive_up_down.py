from django.core.management.base import BaseCommand
from django.db import transaction, connection
from dask import dataframe as dd
from dask.distributed import Client
from ...models import FloorsheetData, ConsecutiveUpDown
import datetime
import pandas as pd
import numpy as np


class Command(BaseCommand):
    help = 'Detects stocks with consecutive price increases or decreases (3+ days)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--min-days',
            type=int,
            default=3,
            help='Minimum consecutive days to detect (default: 3)'
        )
        parser.add_argument(
            '--lookback-days',
            type=int,
            default=120,
            help='Number of days to analyze (default: 30)'
        )

    def handle(self, *args, **options):
        self.stdout.write("Starting consecutive up/down days detection...")
        
        min_days = options['min_days']
        lookback_days = options['lookback_days']
        
        # Calculate date range
        end_date = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=lookback_days)
        
        client = None
        try:
            client = Client(processes=False)
            
            with connection.cursor() as cursor:
                # Get daily closing prices (last rate for each symbol each day)
                query = f"""
                    SELECT 
                        symbol,
                        date,
                        LAST_VALUE(rate) OVER (
                            PARTITION BY symbol, date 
                            ORDER BY transaction_no
                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                        ) as closing_rate
                    FROM {FloorsheetData._meta.db_table}
                    WHERE date BETWEEN %s AND %s
                    GROUP BY symbol, date, transaction_no, rate
                """
                cursor.execute(query, [start_date, end_date])
                data = cursor.fetchall()
                
                if not data:
                    self.stdout.write("No data found for the given date range.")
                    return
                
                # Create DataFrame
                columns = ['symbol', 'date', 'closing_rate']
                df = pd.DataFrame(data, columns=columns)
                
                # Convert to Dask DataFrame for processing
                ddf = dd.from_pandas(df, npartitions=4)
                
                # Process data to find streaks
                def find_streaks(group):
                    group = group.sort_values('date')
                    group['price_change'] = group['closing_rate'].diff()
                    group['direction'] = np.where(
                        group['price_change'] > 0, 1, 
                        np.where(group['price_change'] < 0, -1, 0))
                    
                    # Identify streak changes
                    group['streak_change'] = group['direction'].ne(group['direction'].shift())
                    group['streak_id'] = group['streak_change'].cumsum()
                    
                    # Calculate streak lengths
                    streaks = group.groupby(['symbol', 'direction', 'streak_id'])\
                        .agg(
                            start_date=('date', 'min'),
                            end_date=('date', 'max'),
                            days=('date', 'count'),
                            start_price=('closing_rate', 'first'),
                            end_price=('closing_rate', 'last')
                        ).reset_index()
                    
                    return streaks[streaks['days'] >= min_days]
                
                # Apply streak detection per symbol
                result = ddf.groupby('symbol')\
                    .apply(find_streaks, meta={
                        'symbol': 'object',
                        'direction': 'int64',
                        'streak_id': 'int64',
                        'start_date': 'datetime64[ns]',
                        'end_date': 'datetime64[ns]',
                        'days': 'int64',
                        'start_price': 'float64',
                        'end_price': 'float64'
                    })\
                    .compute()
                
                # Prepare records for saving
                records_to_save = []
                for _, row in result.iterrows():
                    movement_type = 'UP' if row['direction'] == 1 else 'DOWN'
                    percent_change = (
                        (row['end_price'] - row['start_price']) / row['start_price'] * 100
                    )
                    
                    records_to_save.append(ConsecutiveUpDown(
                        symbol=row['symbol'],
                        movement_type=movement_type,
                        start_date=row['start_date'],
                        end_date=row['end_date'],
                        days_count=row['days'],
                        start_price=row['start_price'],
                        end_price=row['end_price'],
                        percent_change=percent_change
                    ))
                
                # Save to database
                with transaction.atomic():
                    ConsecutiveUpDown.objects.all().delete()
                    ConsecutiveUpDown.objects.bulk_create(records_to_save)
                
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Found {len(records_to_save)} consecutive price movements "
                        f"({min_days}+ days)"
                    )
                )
                
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"Error detecting price movements: {str(e)}")
            )
            raise
        finally:
            if client:
                client.close()
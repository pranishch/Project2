from django.core.management.base import BaseCommand
from django.db import transaction, connection
from dask import dataframe as dd
from dask.distributed import Client
from ...models import FloorsheetData, ContinuousLowVolume
import datetime
import pandas as pd
import sys
from django.db.utils import OperationalError


class Command(BaseCommand):
    help = 'Detects stocks with continuous low trading volume (<1000 shares)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--min-days',
            type=int,
            default=4,  # More flexible default (originally 10)
            help='Minimum consecutive days with volume <1000 (default: 7)'
        )
        parser.add_argument(
            '--days-back',
            type=int,
            default=240,  # Extended lookback period (originally 90)
            help='Days to analyze (default: 120)'
        )
        parser.add_argument(
            '--ignore-gaps',
            action='store_true',
            help='Ignore non-trading days in streak calculations'
        )
        parser.add_argument(
            '--show-examples',
            type=int,
            default=0,
            help='Show N examples of low-volume days (debugging)'
        )

    def handle(self, *args, **options):
        min_days = options['min_days']
        days_back = options['days_back']
        ignore_gaps = options['ignore_gaps']
        show_examples = options['show_examples']

        # Date range calculation
        end_date = datetime.date.today()
        start_date = end_date - datetime.timedelta(days=days_back)
        
        self.stdout.write(
            f"Detecting stocks with volume <1000 for {min_days}+ days "
            f"from {start_date} to {end_date}"
        )

        try:
            with Client(processes=False) as client, connection.cursor() as cursor:
                # Check data existence first
                cursor.execute(
                    f"SELECT EXISTS(SELECT 1 FROM {FloorsheetData._meta.db_table} "
                    "WHERE date BETWEEN %s AND %s AND quantity < 1000 LIMIT 1)",
                    [start_date, end_date]
                )
                if not cursor.fetchone()[0]:
                    self.stdout.write(
                        self.style.WARNING("No trades <1000 shares found in date range")
                    )
                    return

                # Main data query (optimized)
                sql = f"""
                    WITH low_volume_days AS (
                        SELECT 
                            symbol, 
                            date, 
                            SUM(quantity) as total_quantity
                        FROM {FloorsheetData._meta.db_table}
                        WHERE date BETWEEN %s AND %s
                        GROUP BY symbol, date
                        HAVING SUM(quantity) < 1000
                    )
                    SELECT * FROM low_volume_days ORDER BY symbol, date
                """
                cursor.execute(sql, [start_date, end_date])
                
                # Convert to DataFrame
                df = pd.DataFrame(
                    cursor.fetchall(),
                    columns=['symbol', 'date', 'total_quantity']
                )
                df['date'] = pd.to_datetime(df['date'])

                if show_examples > 0:
                    self.stdout.write(f"\nSample low-volume records ({show_examples}):")
                    self.stdout.write(
                        df.sample(min(show_examples, len(df))
                        .to_string(index=False)
                    ))

                # Streak detection with gap handling
                df['prev_date'] = df.groupby('symbol')['date'].shift(1)
                df['gap'] = (df['date'] - df['prev_date']).dt.days
                
                if ignore_gaps:
                    df['streak_id'] = (
                        (df['gap'] > 3) |  # More than 3 days gap
                        df['gap'].isna()   # First record
                    ).cumsum()
                else:
                    df['streak_id'] = (
                        (df['gap'] > 1) |  # Any gap
                        df['gap'].isna()
                    ).cumsum()

                # Calculate streaks
                streaks = df.groupby(['symbol', 'streak_id']).agg(
                    start_date=('date', 'min'),
                    end_date=('date', 'max'),
                    days=('date', 'count'),
                    avg_volume=('total_quantity', 'mean')
                ).reset_index()

                # Filter significant streaks
                streaks = streaks[streaks['days'] >= min_days]
                
                if streaks.empty:
                    self.stdout.write(
                        self.style.WARNING(
                            f"No stocks found with volume <1000 for {min_days}+ consecutive days.\n"
                            "Suggestions:\n"
                            f"1. Increase --days-back (current: {days_back})\n"
                            f"2. Reduce --min-days (current: {min_days})\n"
                            "3. Check data quality with --show-examples=10"
                        )
                    )
                    return

                # Save results
                with transaction.atomic():
                    ContinuousLowVolume.objects.all().delete()
                    ContinuousLowVolume.objects.bulk_create([
                        ContinuousLowVolume(
                            symbol=row['symbol'],
                            start_date=row['start_date'].date(),
                            end_date=row['end_date'].date(),
                            consecutive_days=row['days'],
                            avg_volume=row['avg_volume'],
                            max_volume_threshold=1000
                        ) for _, row in streaks.iterrows()
                    ])

                self.stdout.write(
                    self.style.SUCCESS(
                        f"Found {len(streaks)} stocks meeting criteria\n"
                        "Top 5 by streak length:\n" +
                        streaks.nlargest(5, 'days').to_string(index=False)
                    )
                )

        except OperationalError as e:
            self.stdout.write(self.style.ERROR(f"Database error: {e}"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Processing error: {e}"))
            if show_examples:
                import traceback
                self.stdout.write(traceback.format_exc())
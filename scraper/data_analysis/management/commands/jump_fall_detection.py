from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
import dask.dataframe as dd
import pandas as pd
from datetime import datetime
from dask.distributed import Client
from data_analysis.models import FloorsheetData, JumpFallDetection

class Command(BaseCommand):
    help = 'Detects price jumps/falls (>X%) for the latest trading day'

    def add_arguments(self, parser):
        parser.add_argument(
            '--threshold',
            type=float,
            default=5.0,
            help='Percentage threshold for movement (default: 5%%)'
        )

    def handle(self, *args, **options):
        threshold = options['threshold']
        
        try:
            # Get the latest date available in the database
            latest_date = FloorsheetData.objects.latest('date').date
            self.stdout.write(f"Analyzing price movements for {latest_date}...")
            
            with Client() as client:
                # Load only the latest date's data
                self.stdout.write("Loading data...")
                df = self.load_floorsheet_data(latest_date)
                
                if df is None or len(df) == 0:
                    self.stdout.write(self.style.WARNING(f"No data found for {latest_date}"))
                    return
                
                # Calculate movements
                self.stdout.write("Calculating price changes...")
                movements = self.calculate_movements(df, threshold)
                
                # Save results
                with transaction.atomic():
                    # Clear previous results
                    JumpFallDetection.objects.all().delete()
                    
                    # Save new movements
                    saved = self.save_movements(movements)
                    self.stdout.write(self.style.SUCCESS(
                        f"Found {saved} stocks with >={threshold}% movement on {latest_date}"
                    ))
                    
        except FloorsheetData.DoesNotExist:
            self.stdout.write(self.style.ERROR("No floorsheet data available"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Processing error: {str(e)}"))

    def load_floorsheet_data(self, target_date):
        """Load data for specific date using Dask"""
        ddf = dd.from_pandas(
            pd.DataFrame(
                list(FloorsheetData.objects.filter(
                    date=target_date
                ).values(
                    'symbol', 'rate', 'date'
                )),
                columns=['symbol', 'rate', 'date']
            ),
            npartitions=2  # Fewer partitions for single-day data
        )
        ddf['date'] = dd.to_datetime(ddf['date'])
        return ddf

    def calculate_movements(self, ddf, threshold):
        """Calculate price movements for the latest date"""
        # Get daily min/max prices per symbol
        daily_prices = ddf.groupby(['symbol', 'date']).agg(
            min_price=('rate', 'min'),
            max_price=('rate', 'max'),
            first_price=('rate', 'first'),
            last_price=('rate', 'last')
        ).compute()
        
        # Calculate both intraday and open-close movements
        daily_prices['intraday_change_pct'] = (
            (daily_prices['max_price'] - daily_prices['min_price']) / 
            daily_prices['min_price'] * 100
        )
        daily_prices['open_close_change_pct'] = (
            (daily_prices['last_price'] - daily_prices['first_price']) / 
            daily_prices['first_price'] * 100
        )
        
        # Filter for significant movements
        significant_moves = daily_prices[
            (daily_prices['intraday_change_pct'].abs() >= threshold) |
            (daily_prices['open_close_change_pct'].abs() >= threshold)
        ].reset_index()
        
        # Classify movement direction based on larger change
        significant_moves['movement_type'] = significant_moves.apply(
            lambda x: 'jump' if max(x['intraday_change_pct'], x['open_close_change_pct']) > 0 else 'fall',
            axis=1
        )
        significant_moves['percentage_change'] = significant_moves.apply(
            lambda x: max(abs(x['intraday_change_pct']), abs(x['open_close_change_pct'])),
            axis=1
        )
        
        return significant_moves[['symbol', 'date', 'movement_type', 'percentage_change',
                                'first_price', 'last_price', 'min_price', 'max_price']]

    def save_movements(self, movements_df):
        """Save movements to database"""
        if movements_df is None or len(movements_df) == 0:
            return 0
            
        records = [
            JumpFallDetection(
                symbol=row['symbol'],
                date=row['date'],
                movement_type=row['movement_type'],
                percentage_change=row['percentage_change'],
                open_price=row['first_price'],
                close_price=row['last_price'],
                low_price=row['min_price'],
                high_price=row['max_price']
            )
            for _, row in movements_df.iterrows()
        ]
        JumpFallDetection.objects.bulk_create(records)
        return len(records)
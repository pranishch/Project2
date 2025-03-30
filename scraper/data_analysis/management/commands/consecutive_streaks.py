from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
import dask.dataframe as dd
import pandas as pd
from datetime import datetime, timedelta
from dask.distributed import Client
from data_analysis.models import FloorsheetData, ConsecutiveStreak

class Command(BaseCommand):
    help = 'Detects continuous buying/selling streaks (5+ days) by brokers'

    def add_arguments(self, parser):
        parser.add_argument(
            '--days-back',
            type=int,
            default=365,
            help='Number of days to look back (default: 365)'
        )
        parser.add_argument(
            '--min-streak',
            type=int,
            default=5,  # Default changed to 5 days
            help='Minimum streak length to report (default: 5)'
        )

    def handle(self, *args, **options):
        days_back = options['days_back']
        min_streak = options['min_streak']  # Now defaults to 5
        cutoff_date = (timezone.now() - timedelta(days=days_back)).date()
        
        self.stdout.write(f"Detecting trading streaks ({min_streak}+ days) since {cutoff_date}...")
        
        try:
            with Client() as client:
                # Load data
                self.stdout.write("Loading floorsheet data...")
                df = self.load_floorsheet_data(cutoff_date)
                
                if df is None or len(df) == 0:
                    self.stdout.write(self.style.WARNING("No floorsheet data found"))
                    return
                
                # Process streaks
                self.stdout.write("Processing buying streaks...")
                buy_streaks = self.find_streaks(df, 'buyer', min_streak)
                
                self.stdout.write("Processing selling streaks...")
                sell_streaks = self.find_streaks(df, 'seller', min_streak)
                
                # Save results
                self.stdout.write("Saving results...")
                with transaction.atomic():
                    ConsecutiveStreak.objects.all().delete()
                    
                    saved = self.save_streaks(buy_streaks, 'buying') + \
                           self.save_streaks(sell_streaks, 'selling')
                    
                    self.stdout.write(self.style.SUCCESS(
                        f"Saved {saved} trading streaks ({min_streak}+ days)"
                    ))
                    
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error: {str(e)}"))

    def load_floorsheet_data(self, cutoff_date):
        """Load data with proper date handling"""
        ddf = dd.from_pandas(
            pd.DataFrame(
                list(FloorsheetData.objects.filter(
                    date__gte=cutoff_date
                ).values(
                    'symbol', 'buyer', 'seller', 'quantity', 'date'
                )),
                columns=['symbol', 'buyer', 'seller', 'quantity', 'date']
            ),
            npartitions=4
        )
        ddf['date'] = dd.to_datetime(ddf['date'])
        return ddf

    def find_streaks(self, ddf, broker_type, min_streak=5):  # Default set to 5
        """Find consecutive streaks of specified minimum length"""
        grouped = ddf.groupby(['symbol', broker_type])
        
        def process_group(pdf):
            pdf = pdf.sort_values('date')
            pdf['date_diff'] = pdf['date'].diff().dt.days.fillna(1)
            pdf['streak_id'] = (pdf['date_diff'] != 1).cumsum()
            
            streaks = pdf.groupby('streak_id').agg(
                start_date=('date', 'min'),
                end_date=('date', 'max'),
                streak_length=('date', 'count'),
                total_quantity=('quantity', 'sum')
            )
            # Enforce 5+ day streaks by default
            streaks = streaks[streaks['streak_length'] >= min_streak]
            
            if streaks.empty:
                return None
                
            streaks['symbol'] = pdf['symbol'].iloc[0]
            streaks['broker'] = pdf[broker_type].iloc[0]
            return streaks[['symbol', 'broker', 'start_date', 'end_date', 'streak_length', 'total_quantity']]
        
        result = grouped.apply(
            process_group,
            meta={
                'symbol': 'object',
                'broker': 'object',
                'start_date': 'datetime64[ns]',
                'end_date': 'datetime64[ns]',
                'streak_length': 'int64',
                'total_quantity': 'float64'
            }
        ).compute()
        
        return result.dropna() if result is not None else None

    def save_streaks(self, streaks_df, streak_type):
        """Save all detected streaks"""
        if streaks_df is None or len(streaks_df) == 0:
            return 0
            
        records = [
            ConsecutiveStreak(
                symbol=row['symbol'],
                broker=row['broker'],
                streak_type=streak_type,
                start_date=row['start_date'],
                end_date=row['end_date'],
                streak_length=row['streak_length'],
                total_quantity=row['total_quantity']
            )
            for _, row in streaks_df.iterrows()
        ]
        ConsecutiveStreak.objects.bulk_create(records)
        return len(records)
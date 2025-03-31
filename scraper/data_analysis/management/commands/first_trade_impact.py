from django.core.management.base import BaseCommand
from data_analysis.models import FirstTradeImpact, FloorsheetData
import pandas as pd
from datetime import timedelta
from django.db import transaction

class Command(BaseCommand):
    help = 'Detects significant price fluctuations on the first trade of the day and stores in the database.'

    def handle(self, *args, **kwargs):
        self.stdout.write("Starting first trade impact analysis...")

        THRESHOLD = 0.03  # 3% threshold for significant impact

        queryset = FloorsheetData.objects.values('symbol', 'date', 'rate', 'transaction_no')
        df = pd.DataFrame(list(queryset))

        if df.empty:
            self.stdout.write(self.style.WARNING("No data available for analysis."))
            return

        # Convert date columns to datetime
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(['symbol', 'date', 'transaction_no'])

        impactful_trades = self.first_trade_impact(df, threshold=THRESHOLD)

        if impactful_trades.empty:
            self.stdout.write(self.style.WARNING("No impactful trades detected."))
            return

        impact_records = [
            FirstTradeImpact(
                symbol=row['symbol'],
                date=row['date'].date(),  # Convert to Python date object
                rate=row['rate'],
                prev_close=row['prev_close'],
                price_change=row['price_change'],
                impact=row['impact']
            )
            for _, row in impactful_trades.iterrows()
        ]

        with transaction.atomic():
            FirstTradeImpact.objects.all().delete()  # Clear existing data
            FirstTradeImpact.objects.bulk_create(impact_records)

        self.stdout.write(self.style.SUCCESS(f"Analysis completed. {len(impact_records)} records saved."))

    def first_trade_impact(self, df, threshold=0.03):
        # Get first trade of each day for each symbol
        first_trades = df.groupby(['symbol', 'date']).first().reset_index()
        
        # Calculate previous day's closing price
        prev_day_close = df.groupby(['symbol', 'date'])['rate'].last().reset_index()
        prev_day_close.rename(columns={'rate': 'prev_close'}, inplace=True)
        
        # Create previous date reference by shifting dates
        prev_day_close['next_date'] = prev_day_close['date'] + pd.DateOffset(days=1)
        
        # Merge with first trades using proper date matching
        result = pd.merge(
            first_trades,
            prev_day_close[['symbol', 'next_date', 'prev_close']],
            left_on=['symbol', 'date'],
            right_on=['symbol', 'next_date'],
            how='left'
        )
        
        # Calculate price change and impact
        result['price_change'] = (result['rate'] - result['prev_close']) / result['prev_close']
        result['impact'] = result['price_change'].apply(
            lambda x: 'Jump' if x >= threshold else ('Drop' if x <= -threshold else 'Stable')
        )
        
        # Return only impactful trades
        impactful_trades = result[result['impact'] != 'Stable']
        return impactful_trades[['symbol', 'date', 'rate', 'prev_close', 'price_change', 'impact']]
from django.core.management.base import BaseCommand
from data_analysis.models import FloorsheetData, TradingHalt
import pandas as pd
from datetime import timedelta

class Command(BaseCommand):
    help = 'Detects stocks where price fluctuations exceed 10%, indicating a possible trading halt'

    def handle(self, *args, **kwargs):
        self.stdout.write("Starting trading halt analysis...")

        THRESHOLD = 10  # 10% price change threshold

        # Get the latest date and define the start date (e.g., last 30 days)
        latest_date = FloorsheetData.objects.latest('date').date
        start_date = latest_date - timedelta(days=30)

        # Fetch trading data
        queryset = FloorsheetData.objects.filter(date__gte=start_date, date__lte=latest_date).values(
            'symbol', 'date', 'rate'
        )
        df = pd.DataFrame(list(queryset))

        if df.empty:
            self.stdout.write(self.style.WARNING("No data available for analysis."))
            return

        # Get the opening price (first trade of the day)
        open_price = df.groupby(['symbol', 'date'])['rate'].first().reset_index(name='open_price')

        # Merge opening price with the original data
        trading_halt_report = df.merge(open_price, on=['symbol', 'date'], how='inner', suffixes=('', '_open'))

        # Calculate price change percentage
        trading_halt_report['price_change_pct'] = (
            (trading_halt_report['rate'] - trading_halt_report['open_price']) / trading_halt_report['open_price']
        ) * 100

        # Filter stocks with price changes above the threshold
        trading_halt_report = trading_halt_report[abs(trading_halt_report['price_change_pct']) >= THRESHOLD]

        if trading_halt_report.empty:
            self.stdout.write(self.style.WARNING("No trading halt detected."))
            return

        # Prepare data for bulk insertion
        halt_records = [
            TradingHalt(
                symbol=row['symbol'],
                date=row['date'],
                rate=row['rate'],
                open_price=row['open_price'],
                price_change_pct=row['price_change_pct']
            )
            for _, row in trading_halt_report.iterrows()
        ]

        # Save results in bulk
        TradingHalt.objects.bulk_create(halt_records)

        self.stdout.write(self.style.SUCCESS(f"Analysis completed. {len(halt_records)} records saved."))
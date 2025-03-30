from django.core.management.base import BaseCommand
from data_analysis.models import FloorsheetData, IntradayBigTrade
import pandas as pd
from datetime import timedelta

class Command(BaseCommand):
    help = 'Detects intraday big trades where a single trade exceeds 10% of total daily volume'

    def handle(self, *args, **kwargs):
        self.stdout.write("Starting intraday big trade analysis...")

        THRESHOLD = 0.10  # 10% of daily volume
        
        # Get the latest date and define the start date (e.g., last 30 days)
        latest_date = FloorsheetData.objects.latest('date').date
        start_date = latest_date - timedelta(days=30)
        
        # Fetch trade data within the selected period
        queryset = FloorsheetData.objects.filter(date__gte=start_date, date__lte=latest_date).values(
            'symbol', 'date', 'buyer', 'seller', 'quantity', 'rate'
        )
        df = pd.DataFrame(list(queryset))

        if df.empty:
            self.stdout.write(self.style.WARNING("No data available for analysis."))
            return

        # Compute total daily volume for each stock
        total_volume_daily = df.groupby(['symbol', 'date'])['quantity'].sum().reset_index(name='total_volume_daily')

        # Merge with original data
        big_trade_report = df.merge(total_volume_daily, on=['symbol', 'date'])

        # Filter trades exceeding 10% of total daily volume
        big_trade_report = big_trade_report[big_trade_report['quantity'] > THRESHOLD * big_trade_report['total_volume_daily']]
        
        if big_trade_report.empty:
            self.stdout.write(self.style.WARNING("No significant intraday big trades detected."))
            return

        # Prepare data for bulk insertion
        trade_records = [
            IntradayBigTrade(
                symbol=row['symbol'],
                date=row['date'],
                buyer=row['buyer'] or 'UNKNOWN',
                seller=row['seller'] or 'UNKNOWN',
                quantity=row['quantity'],
                rate=row['rate'],
                total_volume_daily=row['total_volume_daily']
            )
            for _, row in big_trade_report.iterrows()
        ]

        # Save results in bulk
        IntradayBigTrade.objects.bulk_create(trade_records)

        self.stdout.write(self.style.SUCCESS(f"Analysis completed. {len(trade_records)} records saved."))
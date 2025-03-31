from django.core.management.base import BaseCommand
from data_analysis.models import FloorsheetData, LowVolatilityStocks
import pandas as pd

class Command(BaseCommand):
    help = 'Identifies low-volatility stocks'

    def handle(self, *args, **kwargs):
        self.stdout.write("Starting analysis for low-volatility stocks...")

        queryset = FloorsheetData.objects.all().values('symbol', 'date', 'rate')
        df = pd.DataFrame(list(queryset))

        if df.empty:
            self.stdout.write(self.style.WARNING("No data available for analysis."))
            return

        low_volatility_stocks = self.low_volatility_stocks(df)

        if low_volatility_stocks.empty:
            self.stdout.write(self.style.WARNING("No low-volatility stocks found."))
            return

        self.save_to_db(low_volatility_stocks)

        self.stdout.write(self.style.SUCCESS(f"Analysis completed. {len(low_volatility_stocks)} records saved."))

    def low_volatility_stocks(self, df, threshold=0.01):
        daily_prices = df.groupby(['symbol', 'date'])['rate'].agg(['max', 'min']).reset_index()
        daily_prices.rename(columns={'max': 'high', 'min': 'low'}, inplace=True)

        daily_prices['volatility'] = (daily_prices['high'] - daily_prices['low']) / daily_prices['high']

        low_volatility_df = daily_prices[daily_prices['volatility'] <= threshold].copy()

        return low_volatility_df.sort_values(by='volatility', ascending=True)

    def save_to_db(self, low_volatility_stocks):
        records = [
            LowVolatilityStocks(
                symbol=row['symbol'],
                date=row['date'],
                high=row['high'],
                low=row['low'],
                volatility=row['volatility']
            )
            for _, row in low_volatility_stocks.iterrows()
        ]
        LowVolatilityStocks.objects.bulk_create(records)
from django.core.management.base import BaseCommand
from data_analysis.models import FloorsheetData, Stock52WeekExtremes
import pandas as pd

class Command(BaseCommand):
    help = 'Identifies stocks near their 52-week high or low'

    def handle(self, *args, **kwargs):
        self.stdout.write("Starting analysis for stocks near 52-week extremes...")

        queryset = FloorsheetData.objects.all().values('symbol', 'date', 'rate')
        df = pd.DataFrame(list(queryset))

        if df.empty:
            self.stdout.write(self.style.WARNING("No data available for analysis."))
            return

        stocks_near_extremes = self.stocks_near_52_week_extremes(df)

        if stocks_near_extremes.empty:
            self.stdout.write(self.style.WARNING("No stocks near 52-week extremes found."))
            return

        self.save_to_db(stocks_near_extremes)

        self.stdout.write(self.style.SUCCESS(f"Analysis completed. {len(stocks_near_extremes)} records saved."))

    def stocks_near_52_week_extremes(self, df, high_threshold=0.05, low_threshold=0.05):
        df['date'] = pd.to_datetime(df['date'])

        latest_prices = df.groupby('symbol')['date'].max().reset_index()
        latest_prices = latest_prices.merge(df[['symbol', 'date', 'rate']], on=['symbol', 'date'], how='left')
        latest_prices.rename(columns={'rate': 'latest_price'}, inplace=True)

        one_year_ago = df['date'].max() - pd.DateOffset(weeks=52)
        yearly_data = df[df['date'] >= one_year_ago]

        yearly_high_low = yearly_data.groupby('symbol')['rate'].agg(['max', 'min']).reset_index()
        yearly_high_low.rename(columns={'max': '52_week_high', 'min': '52_week_low'}, inplace=True)

        result = latest_prices.merge(yearly_high_low, on='symbol')

        result['near_high'] = ((result['52_week_high'] - result['latest_price']) / result['52_week_high']) <= high_threshold
        result['near_low'] = ((result['latest_price'] - result['52_week_low']) / result['52_week_low']) <= low_threshold

        result = result[(result['near_high'] | result['near_low'])]

        result['status'] = result.apply(lambda x: 'Near High' if x['near_high'] else 'Near Low', axis=1)

        return result[['symbol', 'date', 'latest_price', '52_week_high', '52_week_low', 'status']]

    def save_to_db(self, stocks_near_extremes):
        records = [
            Stock52WeekExtremes(
                symbol=row['symbol'],
                date=row['date'],
                latest_price=row['latest_price'],
                week_52_high=row['52_week_high'],
                week_52_low=row['52_week_low'],
                status=row['status']
            )
            for _, row in stocks_near_extremes.iterrows()
        ]
        Stock52WeekExtremes.objects.bulk_create(records)
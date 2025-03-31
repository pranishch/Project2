from django.core.management.base import BaseCommand
from data_analysis.models import FloorsheetData, BrokerDominance
import pandas as pd

class Command(BaseCommand):
    help = 'Identifies brokers with significant dominance in stock trading'

    def handle(self, *args, **kwargs):
        self.stdout.write("Starting broker dominance analysis...")

        threshold = 0.5  # Default threshold for broker dominance (50%)

        queryset = FloorsheetData.objects.all().values('symbol', 'date', 'buyer', 'seller', 'quantity')
        df = pd.DataFrame(list(queryset))

        if df.empty:
            self.stdout.write(self.style.WARNING("No data available for analysis."))
            return

        dominant_brokers = self.single_broker_dominating(df, threshold)

        if dominant_brokers.empty:
            self.stdout.write(self.style.WARNING("No brokers with significant dominance found."))
            return

        self.save_to_db(dominant_brokers)

        self.stdout.write(self.style.SUCCESS(f"Broker dominance analysis completed. {len(dominant_brokers)} records saved."))

    def single_broker_dominating(self, df, threshold=0.5):
        daily_volume = df.groupby(['symbol', 'date'])['quantity'].sum().rename('total_market_volume').reset_index()
        buy_volume = df.groupby(['symbol', 'date', 'buyer'])['quantity'].sum().reset_index()
        buy_volume.rename(columns={'buyer': 'broker', 'quantity': 'buy_volume'}, inplace=True)
        sell_volume = df.groupby(['symbol', 'date', 'seller'])['quantity'].sum().reset_index()
        sell_volume.rename(columns={'seller': 'broker', 'quantity': 'sell_volume'}, inplace=True)
        broker_activity = pd.merge(buy_volume, sell_volume, on=['symbol', 'date', 'broker'], how='outer').fillna(0)
        broker_activity['total_traded_volume'] = broker_activity['buy_volume'] + broker_activity['sell_volume']
        self_trades = df[df['buyer'] == df['seller']].groupby(['symbol', 'date', 'buyer'])['quantity'].sum().reset_index()
        self_trades.rename(columns={'buyer': 'broker', 'quantity': 'self_trade_volume'}, inplace=True)
        broker_activity = broker_activity.merge(self_trades, on=['symbol', 'date', 'broker'], how='left').fillna(0)
        broker_activity['total_volume'] = broker_activity['total_traded_volume'] - broker_activity['self_trade_volume'] + (broker_activity['self_trade_volume'] / 2)
        merged = broker_activity.merge(daily_volume, on=['symbol', 'date'], how='left')
        merged['percentage'] = (merged['total_volume'] / merged['total_market_volume']) * 100
        dominant_brokers = merged[merged['percentage'] > threshold].copy()
        dominant_brokers['role'] = dominant_brokers.apply(
            lambda x: 'Both' if x['buy_volume'] > 0 and x['sell_volume'] > 0 else 
                      ('Buyer' if x['buy_volume'] > 0 else 'Seller'), axis=1
        )
        dominant_brokers = dominant_brokers.sort_values(['percentage', 'total_market_volume'], ascending=[False, False])
        return dominant_brokers[['symbol', 'date', 'broker', 'role', 'buy_volume', 'sell_volume', 'self_trade_volume',
                                 'total_volume', 'total_market_volume', 'percentage']].reset_index(drop=True)

    def save_to_db(self, dominant_brokers):
        records = [
            BrokerDominance(
                symbol=row['symbol'],
                date=row['date'],
                broker=row['broker'],
                role=row['role'],
                buy_volume=row['buy_volume'],
                sell_volume=row['sell_volume'],
                self_trade_volume=row['self_trade_volume'],
                total_volume=row['total_volume'],
                total_market_volume=row['total_market_volume'],
                percentage=row['percentage']
            )
            for _, row in dominant_brokers.iterrows()
        ]
        BrokerDominance.objects.bulk_create(records)
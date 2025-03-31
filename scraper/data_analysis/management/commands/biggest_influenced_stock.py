import dask.dataframe as dd
from django.core.management.base import BaseCommand
from django.db import transaction
from data_analysis.models import FloorsheetData, BiggestInfluencedStock
from datetime import timedelta, datetime
import pandas as pd
import numpy as np


class Command(BaseCommand):
    help = 'Calculates stocks with highest influence on index movement'

    def add_arguments(self, parser):
        parser.add_argument(
            '--top',
            type=int,
            default=10,
            help='Number of top stocks to return (default: 10)'
        )

    def handle(self, *args, **options):
        top_n = options['top']
        
        self.stdout.write(self.style.SUCCESS(f'Calculating top {top_n} stocks by index influence...'))
        
        try:
            latest_date = FloorsheetData.objects.latest('date').date
            previous_date = self.get_previous_trading_day(latest_date)
            
            if not previous_date:
                raise ValueError("No previous trading day available for comparison")
                
            top_stocks = self.calculate_index_influence(latest_date, previous_date, top_n)
            self.save_results(top_stocks, latest_date)
            
            self.stdout.write(
                self.style.SUCCESS(
                    f'Successfully updated top index influencers for {latest_date}!'
                )
            )
        except FloorsheetData.DoesNotExist:
            self.stdout.write(self.style.ERROR('No floorsheet data available!'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error: {str(e)}'))

    def get_previous_trading_day(self, date):
        """Get the most recent trading day before the given date"""
        previous_day = date - timedelta(days=1)
        while previous_day >= date - timedelta(days=7):  # Look back up to 7 days
            if FloorsheetData.objects.filter(date=previous_day).exists():
                return previous_day
            previous_day -= timedelta(days=1)
        return None

    def calculate_index_influence(self, target_date, previous_date, top_n):
        """
        Calculate stocks with highest influence on index movement
        Uses weighted average price and total traded value as proxies
        """
        # Get current day data
        current_qs = FloorsheetData.objects.filter(date=target_date)
        current_df = pd.DataFrame.from_records(
            current_qs.values('symbol', 'amount', 'quantity', 'rate')
        )
        
        # Get previous day data
        previous_qs = FloorsheetData.objects.filter(date=previous_date)
        previous_df = pd.DataFrame.from_records(
            previous_qs.values('symbol', 'amount', 'quantity', 'rate')
        )
        
        # Calculate weighted average price for current day
        current_df['weighted_price'] = current_df['amount'] / current_df['quantity']
        current_agg = current_df.groupby('symbol').agg({
            'weighted_price': 'mean',
            'amount': 'sum',
            'quantity': 'sum'
        }).reset_index()
        current_agg.rename(columns={
            'weighted_price': 'current_price',
            'amount': 'current_amount',
            'quantity': 'current_quantity'
        }, inplace=True)
        
        # Calculate weighted average price for previous day
        previous_df['weighted_price'] = previous_df['amount'] / previous_df['quantity']
        previous_agg = previous_df.groupby('symbol').agg({
            'weighted_price': 'mean',
            'amount': 'sum',
            'quantity': 'sum'
        }).reset_index()
        previous_agg.rename(columns={
            'weighted_price': 'previous_price',
            'amount': 'previous_amount',
            'quantity': 'previous_quantity'
        }, inplace=True)
        
        # Merge current and previous data
        merged_df = pd.merge(
            current_agg,
            previous_agg,
            on='symbol',
            how='inner'  # Only include stocks present on both days
        )
        
        # Calculate price change percentage
        merged_df['price_change_pct'] = (
            (merged_df['current_price'] - merged_df['previous_price']) / 
            merged_df['previous_price']
        ) * 100
        
        # Calculate weight based on traded value (proxy for market cap influence)
        total_traded_value = merged_df['current_amount'].sum()
        merged_df['weight'] = merged_df['current_amount'] / total_traded_value
        
        # Calculate index influence
        merged_df['index_influence'] = merged_df['price_change_pct'] * merged_df['weight']
        
        # Handle potential infinite/NA values
        merged_df.replace([np.inf, -np.inf], np.nan, inplace=True)
        merged_df.dropna(subset=['index_influence'], inplace=True)
        
        # Get top N stocks by absolute index influence
        top_stocks = merged_df.reindex(
            merged_df['index_influence'].abs().sort_values(ascending=False).index
        ).head(top_n)
        
        return top_stocks.set_index('symbol')['index_influence'].to_dict()

    @transaction.atomic
    def save_results(self, top_stocks, calculation_date):
        """
        Save results to database with index influence data
        """
        # Clear existing data for this date
        BiggestInfluencedStock.objects.filter(calculation_date=calculation_date).delete()
        
        # Prepare new records
        records = [
            BiggestInfluencedStock(
                symbol=symbol,
                index_influence=float(influence),
                calculation_date=calculation_date,
                rank=rank+1,
            )
            for rank, (symbol, influence) in enumerate(top_stocks.items())
        ]
        
        # Bulk create new records
        BiggestInfluencedStock.objects.bulk_create(records)
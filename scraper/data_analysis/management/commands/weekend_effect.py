import numpy as np
import pandas as pd
import dask.dataframe as dd
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from ...models import FloorsheetData, WeekendEffect
from datetime import datetime, timedelta

def calculate_weekly_stats(group):
    """Calculate weekly opening and closing prices (standalone function)"""
    sorted_group = group.sort_values('date')
    return pd.Series({
        'symbol': group['symbol'].iloc[0],
        'year': group['year'].iloc[0],
        'week': group['week'].iloc[0],
        'week_start': sorted_group['date'].iloc[0],
        'week_end': sorted_group['date'].iloc[-1],
        'open_price': sorted_group['rate'].iloc[0],
        'close_price': sorted_group['rate'].iloc[-1],
        'weekly_return': (sorted_group['rate'].iloc[-1] - sorted_group['rate'].iloc[0]) / sorted_group['rate'].iloc[0]
    })

class Command(BaseCommand):
    help = 'Analyzes the weekend effect in stock market data'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Starting weekend effect analysis...'))
        
        try:
            # Clear existing results
            WeekendEffect.objects.all().delete()
            
            # Process data and calculate results
            results = self.analyze_weekend_effect()
            
            # Save results to database
            self.save_results(results)
            
            self.stdout.write(self.style.SUCCESS('Successfully completed weekend effect analysis!'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error during analysis: {str(e)}'))
            raise

    def analyze_weekend_effect(self):
        """Analyze the weekend effect using Dask for big data processing"""
        # Create Dask dataframe from Django queryset
        ddf = dd.from_pandas(
            pd.DataFrame(list(
                FloorsheetData.objects.all().values('date', 'symbol', 'rate')
            )),
            npartitions=4
        )

        # Convert date to datetime and extract day of week
        ddf['date'] = dd.to_datetime(ddf['date'])
        ddf['day_of_week'] = ddf['date'].dt.day_name()
        ddf['year'] = ddf['date'].dt.year
        ddf['week'] = ddf['date'].dt.isocalendar().week

        # Group by symbol, year, week to get weekly open/close
        weekly = ddf.groupby(['symbol', 'year', 'week']).apply(
            calculate_weekly_stats,  # Using standalone function instead of method
            meta={
                'symbol': 'object',
                'year': 'int64',
                'week': 'int64',
                'week_start': 'datetime64[ns]',
                'week_end': 'datetime64[ns]',
                'open_price': 'float64',
                'close_price': 'float64',
                'weekly_return': 'float64'
            }
        ).compute()

        # Calculate day of week returns
        daily = ddf.groupby(['symbol', 'date', 'day_of_week']).agg({'rate': 'mean'}).reset_index()
        daily = daily.compute()
        daily['prev_close'] = daily.groupby('symbol')['rate'].shift(1)
        daily['daily_return'] = (daily['rate'] - daily['prev_close']) / daily['prev_close']

        # Calculate average returns by day of week
        day_returns = daily.groupby('day_of_week')['daily_return'].mean().reset_index()
        day_returns = day_returns.sort_values('daily_return', ascending=False)

        # Calculate weekend effect (Monday vs other days)
        monday_returns = daily[daily['day_of_week'] == 'Monday']['daily_return'].mean()
        other_days_returns = daily[daily['day_of_week'] != 'Monday']['daily_return'].mean()
        weekend_effect = monday_returns - other_days_returns

        return {
            'weekly_stats': weekly,
            'day_returns': day_returns,
            'monday_returns': monday_returns,
            'other_days_returns': other_days_returns,
            'weekend_effect': weekend_effect
        }

    @transaction.atomic
    def save_results(self, results):
        """Save analysis results to database"""
        # Save day of week returns
        for _, row in results['day_returns'].iterrows():
            WeekendEffect.objects.create(
                analysis_type='DAY_OF_WEEK',
                day_of_week=row['day_of_week'],
                return_value=row['daily_return'],
                analysis_date=timezone.now()
            )

        # Save weekend effect summary
        WeekendEffect.objects.create(
            analysis_type='WEEKEND_EFFECT',
            day_of_week='Monday',
            return_value=results['weekend_effect'],
            analysis_date=timezone.now(),
            description=f"Monday return: {results['monday_returns']:.4f}, Other days return: {results['other_days_returns']:.4f}"
        )

        # Save weekly stats (sample)
        weekly_sample = results['weekly_stats'].sample(min(100, len(results['weekly_stats'])))
        for _, row in weekly_sample.iterrows():
            WeekendEffect.objects.create(
                analysis_type='WEEKLY_STATS',
                symbol=row['symbol'],
                week_start=row['week_start'],
                week_end=row['week_end'],
                open_price=row['open_price'],
                close_price=row['close_price'],
                return_value=row['weekly_return'],
                analysis_date=timezone.now()
            )
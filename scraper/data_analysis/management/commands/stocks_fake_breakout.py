import os
import pandas as pd
from datetime import datetime, timedelta
from django.core.management.base import BaseCommand
from django.db import transaction
from data_analysis.models import FloorsheetData, StocksFakeBreakout  # Use your actual app name

class Command(BaseCommand):
    help = 'Analyze floorsheet data to identify stocks with fake breakouts'

    def add_arguments(self, parser):
        parser.add_argument(
            '--days',
            type=int,
            default=240,
            help='Number of days to analyze'
        )
        parser.add_argument(
            '--breakout-threshold',
            type=float,
            default=2.0,
            help='Percentage threshold for breakout detection'
        )
        parser.add_argument(
            '--reversal-threshold',
            type=float,
            default=1.5,
            help='Percentage threshold for reversal detection'
        )

    def handle(self, *args, **options):
        days = options['days']
        breakout_threshold = options['breakout_threshold']
        reversal_threshold = options['reversal_threshold']
        
        self.stdout.write(self.style.SUCCESS(f'Starting analysis for {days} days with breakout threshold {breakout_threshold}% and reversal threshold {reversal_threshold}%'))
        
        # Get date range
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        self.stdout.write(self.style.SUCCESS(f'Analyzing data from {start_date} to {end_date}'))
        
        try:
            # Load data directly from Django ORM instead of using Dask SQL
            self.stdout.write(self.style.SUCCESS('Loading data from FloorsheetData model'))
            
            # Get data from Django ORM
            queryset = FloorsheetData.objects.filter(
                date__gte=start_date,
                date__lte=end_date
            ).values('symbol', 'date', 'quantity', 'rate', 'amount')
            
            # Convert to pandas DataFrame
            # For large datasets, use chunking to avoid memory issues
            chunk_size = 50000
            all_data = []
            
            # Process in chunks
            self.stdout.write(self.style.SUCCESS('Processing data in chunks'))
            offset = 0
            while True:
                chunk = list(queryset[offset:offset+chunk_size])
                if not chunk:
                    break
                    
                df_chunk = pd.DataFrame(chunk)
                all_data.append(df_chunk)
                offset += chunk_size
                self.stdout.write(self.style.SUCCESS(f'Processed {offset} rows'))
            
            # Combine all chunks
            if all_data:
                df = pd.concat(all_data, ignore_index=True)
                self.stdout.write(self.style.SUCCESS(f'Successfully loaded {len(df)} rows into DataFrame'))
                
                # Process data
                result = self.process_data(df, breakout_threshold, reversal_threshold)
                
                # Save results to database
                self.save_results(result)
            else:
                self.stdout.write(self.style.WARNING('No data found for the specified date range'))
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error processing data: {str(e)}'))
            raise
    
    def process_data(self, df, breakout_threshold, reversal_threshold):
        self.stdout.write(self.style.SUCCESS('Processing data to identify fake breakouts'))
        
        # Group by symbol and date, then calculate daily metrics
        daily_data = df.groupby(['symbol', 'date']).agg({
            'rate': ['min', 'max', 'mean', 'first', 'last'],
            'quantity': 'sum',
            'amount': 'sum'
        }).reset_index()
        
        # Flatten the MultiIndex columns
        daily_data.columns = [
            '_'.join(col).strip('_') if isinstance(col, tuple) else col
            for col in daily_data.columns
        ]
        
        # Sort by symbol and date
        daily_data = daily_data.sort_values(['symbol', 'date'])
        
        # Calculate metrics for each symbol
        symbols = daily_data['symbol'].unique()
        fake_breakouts = []
        
        for symbol in symbols:
            symbol_data = daily_data[daily_data['symbol'] == symbol].copy()
            
            if len(symbol_data) < 5:  # Need at least 5 days of data
                continue
            
            # Calculate previous day's close and volume
            symbol_data['prev_close'] = symbol_data['rate_last'].shift(1)
            symbol_data['prev_high'] = symbol_data['rate_max'].shift(1)
            symbol_data['prev_low'] = symbol_data['rate_min'].shift(1)
            symbol_data['volume'] = symbol_data['quantity_sum']
            symbol_data['prev_volume'] = symbol_data['volume'].shift(1)
            
            # Calculate 5-day moving averages
            symbol_data['ma5_close'] = symbol_data['rate_last'].rolling(window=5).mean()
            symbol_data['ma5_volume'] = symbol_data['volume'].rolling(window=5).mean()
            
            # Calculate 10-day resistance levels (highest high)
            symbol_data['resistance'] = symbol_data['rate_max'].rolling(window=10).max()
            symbol_data['support'] = symbol_data['rate_min'].rolling(window=10).min()
            
            # Drop rows with NaN values
            symbol_data = symbol_data.dropna()
            
            if len(symbol_data) < 3:  # Need at least 3 valid days after calculations
                continue
            
            # Identify breakouts
            for i in range(1, len(symbol_data) - 1):
                try:
                    today = symbol_data.iloc[i]
                    yesterday = symbol_data.iloc[i-1]
                    tomorrow = symbol_data.iloc[i+1]
                    
                    # Check for breakout above resistance
                    breakout_up = (
                        today['rate_max'] > yesterday['resistance'] * (1 + breakout_threshold/100) and
                        today['volume'] > yesterday['ma5_volume'] * 1.5  # Volume confirmation
                    )
                    
                    # Check for breakout below support
                    breakout_down = (
                        today['rate_min'] < yesterday['support'] * (1 - breakout_threshold/100) and
                        today['volume'] > yesterday['ma5_volume'] * 1.5  # Volume confirmation
                    )
                    
                    # Check for reversal the next day
                    reversal_up = (
                        tomorrow['rate_last'] < today['rate_last'] * (1 - reversal_threshold/100)
                    )
                    
                    reversal_down = (
                        tomorrow['rate_last'] > today['rate_last'] * (1 + reversal_threshold/100)
                    )
                    
                    # Identify fake breakouts
                    if (breakout_up and reversal_up) or (breakout_down and reversal_down):
                        fake_breakouts.append({
                            'symbol': symbol,
                            'date': today['date'],
                            'breakout_type': 'up' if breakout_up else 'down',
                            'breakout_price': today['rate_max'] if breakout_up else today['rate_min'],
                            'reversal_price': tomorrow['rate_last'],
                            'resistance': yesterday['resistance'],
                            'support': yesterday['support'],
                            'breakout_volume': today['volume'],
                            'avg_volume': yesterday['ma5_volume'],
                            'reversal_percentage': (
                                (tomorrow['rate_last'] - today['rate_last']) / today['rate_last'] * 100
                            )
                        })
                except Exception as e:
                    self.stdout.write(self.style.WARNING(f'Error processing symbol {symbol}: {str(e)}'))
                    continue
        
        self.stdout.write(self.style.SUCCESS(f'Found {len(fake_breakouts)} fake breakouts'))
        return fake_breakouts
    
    @transaction.atomic
    def save_results(self, results):
        self.stdout.write(self.style.SUCCESS('Saving results to database'))
        
        # Delete existing data from FakeBreakout model
        try:
            StocksFakeBreakout.objects.all().delete()
            self.stdout.write(self.style.SUCCESS('Deleted existing fake breakout data'))
            
            # Save new results
            for result in results:
                StocksFakeBreakout.objects.create(
                    symbol=result['symbol'],
                    date=result['date'],
                    breakout_type=result['breakout_type'],
                    breakout_price=result['breakout_price'],
                    reversal_price=result['reversal_price'],
                    resistance=result['resistance'],
                    support=result['support'],
                    breakout_volume=result['breakout_volume'],
                    avg_volume=result['avg_volume'],
                    reversal_percentage=result['reversal_percentage']
                )
            
            self.stdout.write(self.style.SUCCESS(f'Successfully saved {len(results)} fake breakout records'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error saving results: {str(e)}'))
            raise
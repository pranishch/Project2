import dask.dataframe as dd
from django.core.management.base import BaseCommand
from django.db import transaction
from data_analysis.models import FloorsheetData, TradingHaltDetection
from datetime import datetime, timedelta
import pandas as pd

class Command(BaseCommand):
    help = 'Detects stocks that hit circuit breakers (±10% price movement)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--circuit-threshold',
            type=float,
            default=0.10,
            help='Price percentage change to trigger circuit (default: 0.10 for 10%)'
        )
        parser.add_argument(
            '--min-turnover',
            type=int,
            default=100000,
            help='Minimum turnover (Rs.) to consider valid circuit (default: 100000)'
        )

    def handle(self, *args, **options):
        circuit_threshold = options['circuit_threshold']
        min_turnover = options['min_turnover']
        
        self.stdout.write(self.style.SUCCESS('Detecting circuit breakers...'))
        
        try:
            latest_date = FloorsheetData.objects.latest('date').date
            previous_date = latest_date - timedelta(days=1)
            
            current_prices = self.get_daily_prices(latest_date)
            previous_prices = self.get_daily_prices(previous_date)
            
            circuit_stocks = self.detect_circuits(
                current_prices, 
                previous_prices,
                circuit_threshold,
                min_turnover
            )
            
            self.save_results(circuit_stocks, latest_date)
            
            self.stdout.write(
                self.style.SUCCESS(
                    f'Found {len(circuit_stocks)} potential circuit breakers for {latest_date}'
                )
            )
        except FloorsheetData.DoesNotExist:
            self.stdout.write(self.style.ERROR('No floorsheet data available!'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error: {str(e)}'))

    def get_daily_prices(self, date):
        """Calculate daily price metrics using Dask"""
        queryset = FloorsheetData.objects.filter(date=date)\
            .values('symbol', 'rate', 'amount')
        
        pandas_df = pd.DataFrame.from_records(queryset)
        ddf = dd.from_pandas(pandas_df, npartitions=2)
        
        # Calculate components for VWAP
        ddf['rate_x_amount'] = ddf['rate'] * ddf['amount']
        
        # Aggregate components
        grouped = ddf.groupby('symbol').agg({
            'amount': 'sum',
            'rate_x_amount': 'sum'
        }).compute()
        
        # Calculate VWAP
        grouped['vwap'] = grouped['rate_x_amount'] / grouped['amount']
        grouped.columns = ['turnover', 'rate_x_amount_sum', 'vwap']
        
        return grouped[['turnover', 'vwap']]

    def detect_circuits(self, current, previous, threshold, min_turnover):
        """Detect circuit breakers based on price movement"""
        circuits = []
        
        valid_stocks = current[current['turnover'] >= min_turnover].index
        
        for symbol in valid_stocks:
            if symbol in previous.index:
                current_vwap = current.loc[symbol, 'vwap']
                previous_vwap = previous.loc[symbol, 'vwap']
                
                if previous_vwap > 0:  # Avoid division by zero
                    pct_change = (current_vwap - previous_vwap) / previous_vwap
                    
                    if abs(pct_change) >= threshold:
                        circuits.append({
                            'symbol': symbol,
                            'current_price': current_vwap,
                            'previous_price': previous_vwap,
                            'pct_change': pct_change * 100,
                            'turnover': current.loc[symbol, 'turnover'],
                            'direction': 'UP' if pct_change > 0 else 'DOWN'
                        })
        
        return circuits

    @transaction.atomic
    def save_results(self, circuit_stocks, detection_date):
        """Save detected circuits to database"""
        TradingHaltDetection.objects.filter(detection_date=detection_date).delete()
        
        records = [
            TradingHaltDetection(
                symbol=circuit['symbol'],
                detection_date=detection_date,
                current_price=circuit['current_price'],
                previous_price=circuit['previous_price'],
                pct_change=circuit['pct_change'],
                direction=circuit['direction'],
                turnover=circuit['turnover'],
                is_circuit=True
            )
            for circuit in circuit_stocks
        ]
        
        TradingHaltDetection.objects.bulk_create(records)
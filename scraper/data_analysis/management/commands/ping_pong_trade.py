import dask.dataframe as dd
import pandas as pd
import numpy as np
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from django.core.exceptions import ValidationError
from data_analysis.models import FloorsheetData, PingPongTrade

class Command(BaseCommand):
    help = 'Ping Pong Trade Detection with 30-Minute Time Window'

    def handle(self, *args, **options):
        # Fetch all floorsheet data
        all_trades = FloorsheetData.objects.all()

        # Convert to DataFrame with explicit column names
        try:
            # Ensure consistent column names and types
            df_data = list(all_trades.values(
                'transaction_no', 
                'symbol', 
                'buyer', 
                'seller', 
                'quantity', 
                'rate', 
                'amount', 
                'date'
            ))
            
            # Convert to pandas DataFrame first
            pdf = pd.DataFrame(df_data, columns=[
                'transaction_no', 
                'symbol', 
                'buyer', 
                'seller', 
                'quantity', 
                'rate', 
                'amount', 
                'date'
            ])

            # Convert to Dask DataFrame
            df = dd.from_pandas(pdf, npartitions=10)
            
            # Ensure correct data types
            df = df.astype({
                'transaction_no': 'string',
                'symbol': 'string',
                'buyer': 'string',
                'seller': 'string',
                'quantity': 'float64',
                'rate': 'float64',
                'amount': 'float64',
                'date': 'datetime64[ns]'
            })

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error preparing data: {str(e)}'))
            return

        # Comprehensive Ping Pong Trade Detection with 30-Minute Window
        def detect_comprehensive_ping_pong_trades(df):
            # Convert to pandas for detailed processing
            df_pandas = df.compute()
            
            # Sort by date and transaction number
            df_sorted = df_pandas.sort_values(['date', 'transaction_no'])
            
            # Store ping pong trade patterns
            ping_pong_patterns = {}

            # Group by symbol
            for symbol, symbol_df in df_sorted.groupby('symbol'):
                # Create sliding window of trades
                for i in range(len(symbol_df) - 1):
                    for j in range(i + 1, len(symbol_df)):
                        trade1 = symbol_df.iloc[i]
                        trade2 = symbol_df.iloc[j]

                        # Calculate time difference in minutes
                        time_diff = (trade2['date'] - trade1['date']).total_seconds() / 60

                        # Comprehensive ping pong detection with 30-minute window
                        is_ping_pong = (
                            trade1['buyer'] == trade2['seller'] and
                            trade1['seller'] == trade2['buyer'] and
                            0 < time_diff <= 30  # Within 30 minutes
                        )

                        if is_ping_pong:
                            # Create a unique key for broker pair
                            broker_pair = tuple(sorted([trade1['buyer'], trade1['seller']]))
                            
                            # Track ping pong trade details
                            if broker_pair not in ping_pong_patterns:
                                ping_pong_patterns[broker_pair] = {
                                    'symbol': symbol,
                                    'trades': [],
                                    'total_quantity': 0,
                                    'total_amount': 0,
                                    'min_time_diff': float('inf'),
                                    'max_time_diff': float('-inf')
                                }
                            
                            ping_pong_patterns[broker_pair]['trades'].append({
                                'transaction1': trade1['transaction_no'],
                                'transaction2': trade2['transaction_no'],
                                'quantity1': trade1['quantity'],
                                'quantity2': trade2['quantity'],
                                'rate1': trade1['rate'],
                                'rate2': trade2['rate'],
                                'date1': trade1['date'],
                                'date2': trade2['date'],
                                'time_diff': time_diff
                            })
                            
                            ping_pong_patterns[broker_pair]['total_quantity'] += (
                                trade1['quantity'] + trade2['quantity']
                            )
                            ping_pong_patterns[broker_pair]['total_amount'] += (
                                trade1['amount'] + trade2['amount']
                            )
                            
                            # Update min and max time differences
                            ping_pong_patterns[broker_pair]['min_time_diff'] = min(
                                ping_pong_patterns[broker_pair]['min_time_diff'], 
                                time_diff
                            )
                            ping_pong_patterns[broker_pair]['max_time_diff'] = max(
                                ping_pong_patterns[broker_pair]['max_time_diff'], 
                                time_diff
                            )

            return ping_pong_patterns

        # Process the trades
        try:
            ping_pong_results = detect_comprehensive_ping_pong_trades(df)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error detecting ping pong trades: {str(e)}'))
            return

        # Save to database with comprehensive analysis
        with transaction.atomic():
            for (broker_a, broker_b), trade_info in ping_pong_results.items():
                try:
                    # Create PingPongTrade entry
                    ping_pong_trade = PingPongTrade.objects.create(
                        broker_a=broker_a,
                        broker_b=broker_b,
                        symbol=trade_info['symbol'],
                        occurrences=len(trade_info['trades']),
                        total_quantity=trade_info['total_quantity'],
                        total_amount=trade_info['total_amount'],
                        detection_date=timezone.now(),
                        is_investigated=False,
                        trade_details=trade_info['trades'],
                        min_time_diff=trade_info['min_time_diff'],
                        max_time_diff=trade_info['max_time_diff']
                    )
                except Exception as e:
                    self.stdout.write(self.style.WARNING(
                        f'Could not save ping pong trade for {broker_a} and {broker_b}: {str(e)}'
                    ))

        # Output comprehensive results
        self.stdout.write(self.style.SUCCESS(
            f'Comprehensive Ping Pong Trade Detection Complete: '
            f'{len(ping_pong_results)} unique broker trade pairs found'
        ))
import os
import pandas as pd
import numpy as np
from django.core.management.base import BaseCommand
from data_analysis.models import FloorsheetData, StocksMovement
from datetime import datetime, date  # Added date import

class Command(BaseCommand):
    help = 'Calculate and store stocks that outperformed the NEPSE index'

    def handle(self, *args, **options):
        # Get the path to the CSV file
        csv_path = os.path.join('data_analysis', r'C:\Users\Arjun\Desktop\project2\scraper\data_analysis\nepse_index_momentum.csv')
        
        try:
            # Load NEPSE index data from CSV
            nepse_df = pd.read_csv(csv_path)
            nepse_df['date'] = pd.to_datetime(nepse_df['date']).dt.date
            
            # Get floorsheet data from database
            queryset = FloorsheetData.objects.all().values(
                'transaction_no', 'symbol', 'date', 'rate'
            )
            stock_df = pd.DataFrame.from_records(queryset)
            
            if stock_df.empty:
                self.stdout.write(self.style.WARNING('No floorsheet data found'))
                return
            
            # Convert date to datetime.date if it's not already
            if not isinstance(stock_df['date'].iloc[0], date):  # Changed to date
                stock_df['date'] = pd.to_datetime(stock_df['date']).dt.date
            
            # Calculate outperforming stocks
            result_df = self.flag_stocks_outperforming_index(stock_df, nepse_df)
            
            if result_df.empty:
                self.stdout.write(self.style.WARNING('No outperforming stocks found'))
                return
            
            # Delete existing records
            StocksMovement.objects.all().delete()
            
            # Save new records
            records = result_df.to_dict('records')
            for record in records:
                StocksMovement.objects.create(
                    date=record['date'],
                    symbol=record['symbol'],
                    stock_price=record['stock_price'],
                    stock_change=record['stock_change'],
                    index_change=record['index_change'],
                    difference=record['difference'],
                    stock_direction=record['stock_direction'],
                    index_direction=record['index_direction'],
                    outperformance_type=record['outperformance_type']
                )
            
            self.stdout.write(self.style.SUCCESS(
                f'Successfully saved {len(records)} outperforming stock records'
            ))
            
        except FileNotFoundError:
            self.stdout.write(self.style.ERROR('NEPSE index CSV file not found'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'An error occurred: {str(e)}'))

    def flag_stocks_outperforming_index(self, stock_df, nepse_df, min_outperformance=1.0):
        """
        Flag stocks that moved more than the market index in percentage terms.
        Example: If NEPSE +1% and stock +5%, flag it (with min_outperformance=1)
        
        Args:
            stock_df: DataFrame with columns ['symbol', 'date', 'rate']
            nepse_df: DataFrame with columns ['date', 'mom_value']
            min_outperformance: Minimum percentage points the stock must exceed index by (default 1)
        
        Returns:
            DataFrame with outperforming stocks and their performance details
        """
        # Calculate daily closing prices for stocks
        stock_prices = (
            stock_df.sort_values(['symbol', 'date', 'transaction_no'])
            .groupby(['symbol', 'date'])['rate']
            .last()
            .unstack('symbol')
        )
        
        # Calculate daily percentage changes for stocks
        stock_returns = stock_prices.pct_change() * 100
        
        # Calculate NEPSE percentage changes
        nepse_df = nepse_df.sort_values('date').set_index('date')
        nepse_returns = nepse_df['mom_value'].pct_change() * 100
        
        # Find outperforming stocks
        results = []
        for date in stock_returns.index:
            if date in nepse_returns.index:
                index_change = nepse_returns.loc[date]
                for symbol in stock_returns.columns:
                    stock_change = stock_returns.loc[date, symbol]
                    if not pd.isna(stock_change):
                        # Check if stock moved more than index by min_outperformance
                        if abs(stock_change) > abs(index_change) + min_outperformance:
                            results.append({
                                'date': date,
                                'symbol': symbol,
                                'stock_price': stock_prices.loc[date, symbol],
                                'stock_change': stock_change,
                                'index_change': index_change,
                                'difference': stock_change - index_change
                            })
        
        # Convert to DataFrame
        result_df = pd.DataFrame(results)
        
        if result_df.empty:
            return result_df
        
        # Add direction flags
        result_df['index_direction'] = np.where(result_df['index_change'] > 0, 'Up', 'Down')
        result_df['stock_direction'] = np.where(result_df['stock_change'] > 0, 'Up', 'Down')
        result_df['outperformance_type'] = np.where(
            result_df['stock_direction'] == result_df['index_direction'],
            'Stronger Move',
            'Opposite Move'
        )
        
        return result_df[
            ['date', 'symbol', 'stock_price', 
             'stock_change', 'index_change', 'difference',
             'stock_direction', 'index_direction', 'outperformance_type']
        ].sort_values(['date', 'difference'], ascending=[True, False])
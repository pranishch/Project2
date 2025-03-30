import dask.dataframe as dd
from django.core.management.base import BaseCommand
from django.db import transaction
from data_analysis.models import FloorsheetData, BiggestInfluencedStock
from datetime import timedelta
import pandas as pd


class Command(BaseCommand):
    help = 'Calculates stocks with highest turnover for the latest trading day'

    def add_arguments(self, parser):
        parser.add_argument(
            '--top',
            type=int,
            default=10,
            help='Number of top stocks to return (default: 10)'
        )

    def handle(self, *args, **options):
        top_n = options['top']
        
        self.stdout.write(self.style.SUCCESS(f'Calculating top {top_n} stocks by turnover...'))
        
        try:
            latest_date = FloorsheetData.objects.latest('date').date
            top_stocks = self.calculate_by_turnover(latest_date, top_n)
            self.save_results(top_stocks, latest_date)
            
            self.stdout.write(
                self.style.SUCCESS(
                    f'Successfully updated top turnover stocks for {latest_date}!'
                )
            )
        except FloorsheetData.DoesNotExist:
            self.stdout.write(self.style.ERROR('No floorsheet data available!'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error: {str(e)}'))

    def calculate_by_turnover(self, target_date, top_n):
        """
        Calculate stocks with highest turnover for specific date
        Returns dict of {symbol: turnover_amount}
        """
        queryset = FloorsheetData.objects.filter(
            date=target_date
        ).values('symbol', 'amount')
        
        pandas_df = pd.DataFrame.from_records(queryset)
        ddf = dd.from_pandas(pandas_df, npartitions=2)
        
        # Calculate total turnover per symbol
        turnover_df = ddf.groupby('symbol')['amount'].sum().compute()
        
        # Get top N stocks by turnover
        top_stocks = turnover_df.nlargest(top_n).to_dict()
        
        return top_stocks

    @transaction.atomic
    def save_results(self, top_stocks, calculation_date):
        """
        Save results to database with turnover data only
        """
        # Clear existing data for this date
        BiggestInfluencedStock.objects.filter(calculation_date=calculation_date).delete()
        
        # Prepare new records
        records = [
            BiggestInfluencedStock(
                symbol=symbol,
                turnover=float(turnover),
                calculation_date=calculation_date,
                rank=rank+1,
            )
            for rank, (symbol, turnover) in enumerate(top_stocks.items())
        ]
        
        # Bulk create new records
        BiggestInfluencedStock.objects.bulk_create(records)
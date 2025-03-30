from django.core.management.base import BaseCommand
from django.db import transaction
from data_analysis.models import FloorsheetData, BrokerTransfer
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd

class Command(BaseCommand):
    help = 'Identifies consistent broker transfer patterns'

    def add_arguments(self, parser):
        parser.add_argument(
            '--min-days',
            type=int,
            default=3,
            help='Minimum consecutive days to flag a transfer pattern (default: 3)'
        )
        parser.add_argument(
            '--min-transactions',
            type=int,
            default=5,
            help='Minimum transactions to flag a pair (default: 5)'
        )

    def handle(self, *args, **options):
        min_days = options['min_days']
        min_transactions = options['min_transactions']
        
        client = Client()
        
        try:
            # Clear existing data
            with transaction.atomic():
                deleted_count, _ = BrokerTransfer.objects.all().delete()
                self.stdout.write(f"Cleared {deleted_count} existing records")

            # Process data
            self.stdout.write(f"Identifying transfer patterns (min {min_days} days, min {min_transactions} transactions)")
            self.analyze_transfers(min_days, min_transactions)
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error: {str(e)}"))
        finally:
            client.close()

    def analyze_transfers(self, min_days, min_transactions):
        """Main analysis logic"""
        # Load data using Dask
        queryset = FloorsheetData.objects.all().values(
            'transaction_no', 'symbol', 'buyer', 'seller', 'quantity', 'date'
        )
        
        df = pd.DataFrame.from_records(queryset)
        if df.empty:
            self.stdout.write(self.style.WARNING("No data found"))
            return
            
        ddf = dd.from_pandas(df, npartitions=4)
        ddf['date'] = dd.to_datetime(ddf['date']).dt.date

        # First calculate transaction counts and quantities
        grouped = ddf.groupby(['symbol', 'buyer', 'seller']).agg({
            'transaction_no': 'count',
            'quantity': 'sum',
            'date': ['min', 'max']
        }).compute()

        # Flatten multi-index columns
        grouped.columns = ['_'.join(col).strip() for col in grouped.columns.values]
        grouped = grouped.reset_index()

        # Now calculate unique days separately (Dask workaround)
        unique_days = ddf.groupby(['symbol', 'buyer', 'seller'])['date'].nunique().compute()
        grouped['days_active'] = grouped.apply(
            lambda row: unique_days.get((row['symbol'], row['buyer'], row['seller']), 0),
            axis=1
        )

        # Filter for significant transfers
        significant = grouped[
            (grouped['days_active'] >= min_days) &
            (grouped['transaction_no_count'] >= min_transactions)
        ].sort_values('quantity_sum', ascending=False)

        # Save results
        with transaction.atomic():
            records = [
                BrokerTransfer(
                    transaction_id=f"{row['symbol']}-{row['buyer']}-{row['seller']}-{idx}",
                    symbol=row['symbol'],
                    selling_broker=row['seller'],
                    buying_broker=row['buyer'],
                    kitta=int(row['quantity_sum']),
                    first_date=row['date_min'],
                    last_date=row['date_max'],
                    transaction_count=row['transaction_no_count'],
                    days_active=row['days_active']
                )
                for idx, row in significant.iterrows()
            ]
            
            BrokerTransfer.objects.bulk_create(records)
            self.stdout.write(self.style.SUCCESS(
                f"Saved {len(records)} broker transfer patterns"
            ))

        # Print sample results
        self.stdout.write("\nTop Transfer Patterns:")
        self.stdout.write("Symbol\tSeller\tBuyer\tKitta\tDays\tTxns")
        self.stdout.write("----------------------------------------")
        for _, row in significant.head(10).iterrows():
            self.stdout.write(
                f"{row['symbol']}\t"
                f"{row['seller']}\t"
                f"{row['buyer']}\t"
                f"{int(row['quantity_sum']):,}\t"
                f"{row['days_active']}\t"
                f"{row['transaction_no_count']}"
            )
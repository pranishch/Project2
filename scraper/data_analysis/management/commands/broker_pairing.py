from django.core.management.base import BaseCommand
from django.db import transaction
from data_analysis.models import FloorsheetData, BrokerPair  
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd
from datetime import datetime, timedelta

class Command(BaseCommand):
    help = 'Identifies high-frequency broker pairs in last 5 trading days'

    def add_arguments(self, parser):
        parser.add_argument(
            '--min-transactions',
            type=int,
            default=5,
            help='Minimum transactions to flag a pair (default: 5)'
        )
        parser.add_argument(
            '--min-quantity',
            type=int,
            default=5000,
            help='Minimum cumulative quantity to flag (default: 5000)'
        )

    def handle(self, *args, **options):
        min_transactions = options['min_transactions']
        min_quantity = options['min_quantity']
        
        client = Client()
        
        try:
            # Hardcoded 5-day analysis
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=5)
            
            self.stdout.write(f"Analyzing last 5 trading days ({start_date} to {end_date})")
            self.stdout.write(f"Criteria: >{min_transactions} transactions AND >{min_quantity} quantity")

            # Load data using Dask
            queryset = FloorsheetData.objects.filter(
                date__gte=start_date,
                date__lte=end_date
            ).values('date', 'quantity', 'buyer', 'seller')
            
            pdf = pd.DataFrame.from_records(queryset)
            if pdf.empty:
                self.stdout.write(self.style.WARNING("No data found for the period."))
                return
                
            ddf = dd.from_pandas(pdf, npartitions=4)
            
            # Calculate pair frequencies
            self.stdout.write("Calculating broker pair frequencies...")
            pair_stats = ddf.groupby(['date', 'buyer', 'seller']).agg({
                'quantity': 'sum',
                'seller': 'count'  # Transaction count
            }).rename(columns={'seller': 'transaction_count'}).reset_index()
            
            # Compute results
            result = pair_stats.compute()
            
            # Filter significant pairs
            significant_pairs = result[
                (result['transaction_count'] >= min_transactions) &
                (result['quantity'] >= min_quantity)
            ].sort_values(['date', 'transaction_count'], ascending=[False, False])
            
            # Clear existing data
            with transaction.atomic():
                self.stdout.write("Clearing existing broker pair data...")
                BrokerPair.objects.all().delete()
                
                # Save new records
                self.stdout.write("Saving significant broker pairs...")
                records = [
                    BrokerPair(
                        date=row['date'],
                        quantity=row['quantity'],
                        transaction_count=row['transaction_count'],
                        buyer_broker=row['buyer'],
                        seller_broker=row['seller']
                    )
                    for _, row in significant_pairs.iterrows()
                ]
                BrokerPair.objects.bulk_create(records)
            
            # Print report
            self.stdout.write("\nDate\t| Qty\t| Txns\t| Buyer\t| Seller")
            self.stdout.write("-------------------------------------------")
            for _, row in significant_pairs.iterrows():
                self.stdout.write(
                    f"{row['date'].strftime('%m/%d')}\t| "
                    f"{int(row['quantity'])}\t| "
                    f"{row['transaction_count']}\t| "
                    f"{row['buyer']}\t| "
                    f"{row['seller']}"
                )
            
            self.stdout.write(self.style.SUCCESS(
                f"\nFound {len(significant_pairs)} significant pairs in last 5 days"
            ))
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error: {str(e)}"))
        finally:
            client.close()
import dask.dataframe as dd
import pandas as pd
from django.core.management.base import BaseCommand
from django.db.models import Max, Q
from ...models import FloorsheetData, AccumulationData
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta

class Command(BaseCommand):
    help = 'Detect accumulation patterns across different time frames using Dask'

    def add_arguments(self, parser):
        parser.add_argument(
            '--symbol',
            type=str,
            help='Process specific symbol only',
            required=False
        )

    def handle(self, *args, **options):
        self.stdout.write("Starting accumulation detection process...")
        
        try:
            # Define time frames with their date ranges (in days)
            time_frames = {
                '1D': {'days': 1, 'label': '1 Day'},
                '1W': {'days': 5, 'label': '1 Week'},
                '1M': {'days': 30, 'label': '1 Month'},
                '3M': {'days': 90, 'label': '3 Months'},
                '6M': {'days': 180, 'label': '6 Months'},
                '1Y': {'days': 365, 'label': '1 Year'},
            }

            # Get latest date from database
            latest_date_result = FloorsheetData.objects.aggregate(Max('date'))
            latest_date = latest_date_result['date__max']
            if not latest_date:
                self.stdout.write(self.style.ERROR('No data found in FloorsheetData'))
                return

            self.stdout.write(f"Latest date in database: {latest_date}")

            # Get all unique symbols if no specific symbol provided
            symbol_filter = options.get('symbol')
            if symbol_filter:
                symbols = [symbol_filter]
            else:
                symbols = FloorsheetData.objects.order_by().values_list('symbol', flat=True).distinct()
                self.stdout.write(f"Processing {len(symbols)} symbols...")

            # Process each time frame for all symbols together
            for tf_code, tf_config in time_frames.items():
                self.stdout.write(f"\nProcessing time frame: {tf_config['label']}")
                
                # Calculate date range for this time frame
                days_back = tf_config['days']
                start_date = latest_date - timedelta(days=days_back)
                date_range = f"Last {days_back} days ({start_date} to {latest_date})"
                
                # Delete existing data for this time frame and date range
                deleted_count, _ = AccumulationData.objects.filter(
                    time_frame=tf_config['label'],
                    date=latest_date
                ).delete()
                self.stdout.write(f"  Deleted {deleted_count} existing records for this time frame")

                # Get all data for this time frame
                qs = FloorsheetData.objects.filter(
                    date__gte=start_date,
                    date__lte=latest_date
                ).values(
                    'symbol', 'transaction_no', 'date', 'quantity', 'rate', 'buyer'
                )
                
                if not qs.exists():
                    self.stdout.write(f"No data found for time frame {tf_config['label']}")
                    continue
                
                # Convert to pandas DataFrame with proper date handling
                df = pd.DataFrame(list(qs))
                if df.empty:
                    continue
                    
                df['date'] = pd.to_datetime(df['date']).dt.date
                
                # Now convert to Dask DataFrame
                ddf = dd.from_pandas(df, npartitions=10)
                
                # Prepare batch create
                accumulation_data = []
                
                # Process each symbol
                for symbol in symbols:
                    self.stdout.write(f"  Processing symbol: {symbol}")
                    
                    # Filter data for current symbol
                    symbol_df = ddf[ddf['symbol'] == symbol]
                    if len(symbol_df.index) == 0:
                        continue
                    
                    # Calculate metrics for current period
                    avg_price = symbol_df['rate'].mean().compute()
                    total_volume = symbol_df['quantity'].sum().compute()
                    
                    # Detect large buyers (top 10% by volume)
                    buyer_stats = symbol_df.groupby('buyer')['quantity'].sum().compute()
                    if not buyer_stats.empty:
                        large_buyers = buyer_stats[buyer_stats > buyer_stats.quantile(0.9)].index.tolist()
                    else:
                        large_buyers = []
                    
                    # Determine remarks
                    remarks = []
                    
                    # Get previous period data for comparison
                    prev_start = start_date - timedelta(days=days_back)
                    prev_qs = FloorsheetData.objects.filter(
                        symbol=symbol,
                        date__gte=prev_start,
                        date__lt=start_date
                    ).values('rate', 'quantity')
                    
                    if prev_qs.exists():
                        prev_df = pd.DataFrame(list(prev_qs))
                        if not prev_df.empty:
                            prev_avg_price = prev_df['rate'].mean()
                            prev_total_volume = prev_df['quantity'].sum()
                            
                            if avg_price < prev_avg_price and total_volume > prev_total_volume:
                                remarks.append("Lower avg price with higher volume")
                    
                    # Check for large buyers
                    if large_buyers:
                        remarks.append("Large buyers detected")
                    
                    # Combine remarks
                    remark = " | ".join(remarks) if remarks else "No accumulation detected"
                    
                    # Prepare record for batch create
                    accumulation_data.append(AccumulationData(
                        symbol=symbol,
                        date=latest_date,
                        time_frame=tf_config['label'],
                        avg_price=float(avg_price),
                        total_volume=float(total_volume),
                        remarks=remark,
                        date_range=date_range
                    ))
                
                # Bulk create all records for this time frame
                if accumulation_data:
                    AccumulationData.objects.bulk_create(accumulation_data)
                    self.stdout.write(f"  Saved {len(accumulation_data)} records for {tf_config['label']} time frame")
            
            self.stdout.write(self.style.SUCCESS("Accumulation detection completed successfully"))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Error processing accumulation: {str(e)}'))
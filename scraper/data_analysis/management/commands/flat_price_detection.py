import dask.dataframe as dd
import pandas as pd
import numpy as np
import logging
from django.core.management.base import BaseCommand
from django.db import transaction
from datetime import timedelta
from data_analysis.models import FloorsheetData, FlatPriceDetection

class Command(BaseCommand):
    help = 'Detect stocks with more than 50 transactions having price changes <2 Rs'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename='flat_price_detection.log'
        )
        self.logger = logging.getLogger(__name__)

    def handle(self, *args, **options):
        self.logger.info("Starting flat price detection")
        try:
            earliest_date = FloorsheetData.objects.earliest('date').date
            latest_date = FloorsheetData.objects.latest('date').date

            timeframes = [
                ('daily', 1),
                ('weekly', 7),
                ('monthly', 30),
                ('quarterly', 90),
                ('semiannual', 180)
            ]

            for name, days in timeframes:
                self.logger.info(f"Processing {name} timeframe")
                self.process_timeframe(earliest_date, latest_date, name, days)

            self.stdout.write(self.style.SUCCESS('Flat price detection completed'))
            self.logger.info("Processing completed successfully")

        except Exception as e:
            error_msg = f"Flat price detection failed: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
            self.stdout.write(self.style.ERROR(error_msg))

    def process_timeframe(self, earliest_date, latest_date, timeframe_name, days_back):
        current_end = latest_date
        current_start = current_end - timedelta(days=days_back)

        while current_start >= earliest_date:
            try:
                data = self.load_data(current_start, current_end)

                if data.npartitions == 0 or data.compute().empty:
                    self.logger.debug(f"No data for {current_start} to {current_end}")
                    current_end = current_start - timedelta(days=1)
                    current_start = current_end - timedelta(days=days_back)
                    continue

                results = self.analyze_flat_prices(data)
                if not results.empty:
                    self.save_results(results, timeframe_name, current_start, current_end)

                current_end = current_start - timedelta(days=1)
                current_start = current_end - timedelta(days=days_back)

            except Exception as e:
                self.logger.error(f"Error processing {current_start} to {current_end}: {str(e)}")
                break

    def load_data(self, start_date, end_date):
        """ Load data from FloorsheetData for a given date range. """
        qs = FloorsheetData.objects.filter(date__range=[start_date, end_date]).values('symbol', 'rate', 'transaction_no')

        df = pd.DataFrame.from_records(qs)
        self.logger.info(f"Loaded data shape: {df.shape}")

        # 🚨 Debug: Print first 5 rows
        print("DEBUG: Loaded Data (First 5 Rows):\n", df.head())

        if df.empty:
            self.logger.warning(f"No data found for range: {start_date} - {end_date}")
            return dd.from_pandas(pd.DataFrame(), npartitions=1)

        # Ensure required columns exist
        for col in ['symbol', 'rate', 'transaction_no']:
            if col not in df.columns:
                self.logger.error(f"Missing column: {col}")
                df[col] = np.nan  # Set missing columns to NaN

        return dd.from_pandas(df, npartitions=4)

    def analyze_flat_prices(self, ddf):
        def _analyze(group):
            if len(group) < 50:
                return None

            sorted_group = group.sort_values('transaction_no')
            price_diffs = sorted_group['rate'].diff().abs()
            max_change = price_diffs.max()

            if max_change < 2:
                return {
                    'symbol': str(group['symbol'].iloc[0]),
                    'total_transactions': int(len(group)),
                    'price_change': float(max_change)
                }
            return None

        try:
            results = []
            groups = ddf.groupby('symbol')

            for symbol, group in groups:
                analysis = _analyze(group.compute())
                if analysis:
                    results.append(analysis)

            # 🚨 Debugging: Check output before saving
            if results:
                print("DEBUG: Analysis Results (First 5 Rows):\n", pd.DataFrame(results).head())

            return pd.DataFrame(results) if results else pd.DataFrame()

        except Exception as e:
            self.logger.error(f"Analysis error: {str(e)}")
            return pd.DataFrame()

    def save_results(self, results, timeframe, start, end):
        date_range = f"{timeframe}: {start.date()} to {end.date()}"

        with transaction.atomic():
            # Delete old entries first
            FlatPriceDetection.objects.filter(date_range=date_range).delete()

            # Prepare and validate new entries
            entries = []
            for _, row in results.iterrows():
                try:
                    if pd.isnull(row['symbol']) or pd.isnull(row['total_transactions']) or pd.isnull(row['price_change']):
                        self.logger.warning(f"Skipping invalid row: {row}")
                        continue

                    entries.append(FlatPriceDetection(
                        symbol=str(row['symbol']),
                        total_transactions=int(row['total_transactions']),
                        price_change=float(row['price_change']),
                        date_range=date_range
                    ))
                except (ValueError, KeyError) as e:
                    self.logger.error(f"Invalid data in row {row}: {str(e)}")
                    continue

            if entries:
                try:
                    FlatPriceDetection.objects.bulk_create(entries)
                    self.logger.info(f"Saved {len(entries)} records for {date_range}")
                except Exception as e:
                    self.logger.error(f"Bulk create failed: {str(e)}")
                    success_count = 0
                    for entry in entries:
                        try:
                            entry.save()
                            success_count += 1
                        except Exception as e:
                            self.logger.error(f"Failed to save {entry}: {str(e)}")
                    self.logger.info(f"Saved {success_count} records individually")

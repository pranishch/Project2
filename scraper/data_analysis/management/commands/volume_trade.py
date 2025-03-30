from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
import dask.dataframe as dd
from dask.distributed import Client
import pandas as pd
from datetime import timedelta
from data_analysis.models import FunOwnership, VolumeTrade

class Command(BaseCommand):
    help = 'Generates volume trade reports for all time frames (Daily, Weekly, Monthly, Quarterly, Yearly)'

    def handle(self, *args, **options):
        # Define time frames with their respective deltas
        time_frames = {
            'Daily': timedelta(days=1),
            'Weekly': timedelta(weeks=1),
            'Monthly': timedelta(days=30),
            'Quarterly': timedelta(days=90),
            'Yearly': timedelta(days=365)
        }

        # Clear all existing reports first
        with transaction.atomic():
            deleted_count = VolumeTrade.objects.all().delete()[0]
            self.stdout.write(f"Cleared {deleted_count} existing reports")

        # Process each time frame
        for time_frame, delta in time_frames.items():
            self.stdout.write(f"Processing {time_frame} report...")
            
            try:
                report_df = self.calculate_volume_trade_report(delta)
                
                if report_df is None or report_df.empty:
                    self.stdout.write(self.style.WARNING(f"No data for {time_frame}"))
                    continue
                
                saved_count = self.save_volume_trade_report(time_frame, report_df)
                self.stdout.write(self.style.SUCCESS(
                    f"Saved {saved_count} {time_frame} records"
                ))
                
            except Exception as e:
                self.stdout.write(self.style.ERROR(
                    f"Failed {time_frame}: {str(e)}"
                ))

    def calculate_volume_trade_report(self, time_delta):
        """Calculate report for the given time delta using Dask"""
        client = Client()
        try:
            cutoff_date = timezone.now().date() - time_delta
            queryset = FunOwnership.objects.filter(
                updated_on__gte=cutoff_date
            ).values(
                'symbol', 'public_shares', 'total_listed_shares'
            )
            
            df = pd.DataFrame.from_records(queryset)
            if df.empty:
                return None
                
            ddf = dd.from_pandas(df, npartitions=4)
            
            # Calculate metrics
            result = ddf.groupby(['symbol']).agg({
                'public_shares': ['sum', 'mean'],
                'total_listed_shares': 'mean'
            }).compute()
            
            # Clean up results
            result.columns = ['_'.join(col).strip() for col in result.columns.values]
            result = result.rename(columns={
                'public_shares_sum': 'trading_volume',
                'public_shares_mean': 'public_share',
                'total_listed_shares_mean': 'avg_total_shares'
            })
            
            result['traded_volume_percent'] = (
                result['trading_volume'] / result['avg_total_shares']
            ) * 100
            result['traded_volume_percent'] = result['traded_volume_percent'].round(2)
            
            return result.reset_index()[['symbol', 'public_share', 
                                      'trading_volume', 'traded_volume_percent']]
            
        finally:
            client.close()

    def save_volume_trade_report(self, time_frame, report_df):
        """Save the report to database"""
        if isinstance(report_df, dd.DataFrame):
            report_df = report_df.compute()
        
        records = [
            VolumeTrade(
                time_frame=time_frame,
                symbol=row['symbol'],
                public_share=row['public_share'],
                trading_volume=row['trading_volume'],
                traded_volume_percent=row['traded_volume_percent'],
                report_date=timezone.now().date()
            ) for _, row in report_df.iterrows()
        ]
        
        return len(VolumeTrade.objects.bulk_create(records))
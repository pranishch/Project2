from django.core.management.base import BaseCommand
from django.db import transaction, models
from django.utils import timezone
import pandas as pd
from datetime import timedelta
from data_analysis.models import FunOwnership, VolumeTrade, FloorsheetData

class Command(BaseCommand):
    help = 'Generates volume trade reports using FloorsheetData and FunOwnership'

    def handle(self, *args, **options):
        time_frames = {
            'Daily': timedelta(days=1),
            'Weekly': timedelta(weeks=1),
            'Monthly': timedelta(days=30),
            'Quarterly': timedelta(days=90),
            'Yearly': timedelta(days=365)
        }

        with transaction.atomic():
            VolumeTrade.objects.all().delete()
            self.stdout.write("✅ Cleared all previous reports")

        for time_frame, delta in time_frames.items():
            self.process_time_frame(time_frame, delta)

    def process_time_frame(self, time_frame, delta):
        """Process data for a single time frame"""
        self.stdout.write(f"\n🔄 Processing {time_frame} report...")
        
        try:
            # Get current public shares (latest snapshot)
            current_shares = self.get_latest_public_shares()
            
            # Get trading activity from floorsheet data
            trading_activity = self.get_floorsheet_trading_activity(delta)
            
            if trading_activity.empty:
                self.stdout.write(f"⚠️ No trading data for {time_frame}")
                return
            
            # Prepare final report
            report_df = self.merge_reports(current_shares, trading_activity)
            self.save_results(time_frame, report_df)
            self.print_report(time_frame, report_df)
            
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Error in {time_frame}: {str(e)}"))

    def get_latest_public_shares(self):
        """Get current public shares from FunOwnership"""
        latest_records = []
        # Get the most recent record for each symbol
        symbols = FunOwnership.objects.values_list('symbol', flat=True).distinct()
        
        for symbol in symbols:
            record = FunOwnership.objects.filter(symbol=symbol).latest('updated_on')
            latest_records.append({
                'symbol': record.symbol,
                'current_public_shares': record.public_shares,
                'total_listed_shares': record.total_listed_shares
            })
            
        return pd.DataFrame(latest_records)

    def get_floorsheet_trading_activity(self, delta):
        """Calculate trading volume from FloorsheetData"""
        cutoff_date = timezone.now().date() - delta
        
        # Aggregate trading volume by symbol
        trading_data = (
            FloorsheetData.objects
            .filter(date__gte=cutoff_date)
            .values('symbol')
            .annotate(
                trading_volume=models.Sum('quantity')
            )
            .order_by('symbol')
        )
        
        # Convert to DataFrame
        trading_df = pd.DataFrame(list(trading_data))
        
        if not trading_df.empty:
            trading_df = trading_df[['symbol', 'trading_volume']]
        
        return trading_df if not trading_df.empty else pd.DataFrame(columns=['symbol', 'trading_volume'])

    def merge_reports(self, current_shares, trading_activity):
        """Merge the two data sources"""
        report_df = pd.merge(
            current_shares,
            trading_activity,
            on='symbol',
            how='left'
        ).fillna(0)  # Fill NA with 0 for symbols with no trading
        
        # Calculate traded percentage
        report_df['traded_volume_percent'] = (
            report_df['trading_volume'] / report_df['total_listed_shares']
        ) * 100
        report_df['traded_volume_percent'] = report_df['traded_volume_percent'].round(2)
        
        return report_df

    def save_results(self, time_frame, report_df):
        """Save results to database"""
        records = [
            VolumeTrade(
                time_frame=time_frame,
                symbol=row['symbol'],
                public_share=row['current_public_shares'],
                trading_volume=row['trading_volume'],
                traded_volume_percent=row['traded_volume_percent'],
                report_date=timezone.now().date()
            ) for _, row in report_df.iterrows() if row['trading_volume'] > 0
        ]
        
        if records:
            VolumeTrade.objects.bulk_create(records)
            self.stdout.write(f"💾 Saved {len(records)} {time_frame} records")
        else:
            self.stdout.write(f"⚠️ No records to save for {time_frame}")

    def print_report(self, time_frame, report_df):
        """Print report in required format"""
        self.stdout.write(f"\n📊 {time_frame.upper()} VOLUME TRADE REPORT")
        self.stdout.write("Script name\tPublic share\tTrading volume\tTraded volume(%)")
        self.stdout.write("-" * 60)
        
        # Filter out symbols with no trading activity
        active_df = report_df[report_df['trading_volume'] > 0]
        
        if active_df.empty:
            self.stdout.write("No trading activity for this period")
            return
            
        for _, row in active_df.iterrows():
            self.stdout.write(
                f"{row['symbol']}\t\t"
                f"{row['current_public_shares']:,.0f}\t\t"
                f"{row['trading_volume']:,.0f}\t\t"
                f"{row['traded_volume_percent']:.2f}%"
            )
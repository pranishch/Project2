from django.core.management.base import BaseCommand
from django.db.models import F, Sum, Max
from data_analysis.models import FloorsheetData, BrokerSelfTradingReport
from django.db import transaction
from django.db.models.expressions import Window
from django.db.models.functions import RowNumber

class Command(BaseCommand):
    help = 'Calculates self-trading summary for each unique broker-symbol combination'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('Calculating broker self-trading summaries...'))
        
        # Clear existing data
        BrokerSelfTradingReport.objects.all().delete()
        
        # Get aggregated self-trading data grouped by broker and symbol
        self_trades = (
            FloorsheetData.objects
            .filter(buyer=F('seller'))
            .exclude(buyer__isnull=True)
            .exclude(buyer='')
            .values('buyer', 'symbol')
            .annotate(
                total_quantity=Sum('quantity'),
                total_amount=Sum('amount'),
                weighted_avg_rate=Sum(F('amount')) / Sum(F('quantity')),  # Proper weighted average
                last_date=Max('date')  # Corrected: Using Django's Max() instead of max()
            )
            .order_by('buyer', 'symbol')
        )
        
        # Prepare records for bulk creation
        report_entries = [
            BrokerSelfTradingReport(
                broker=trade['buyer'],
                symbol=trade['symbol'],
                quantity=trade['total_quantity'],
                amount=trade['total_amount'],
                rate=trade['weighted_avg_rate'],
                date=trade['last_date']
            )
            for trade in self_trades
        ]
        
        # Bulk create in single transaction
        with transaction.atomic():
            BrokerSelfTradingReport.objects.bulk_create(report_entries)
        
        self.stdout.write(
            self.style.SUCCESS(f'Generated {len(report_entries)} broker-symbol self-trading summaries')
        )
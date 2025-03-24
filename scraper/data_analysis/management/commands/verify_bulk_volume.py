from django.core.management.base import BaseCommand
from django.db.models import Count, Max, Sum
from datetime import timedelta
from data_analysis.models import FloorsheetData, BulkVolumeTrade

class Command(BaseCommand):
    help = "Verify if all buy brokers with highest quantity trades are processed correctly"

    def handle(self, *args, **options):
        latest_date = FloorsheetData.objects.aggregate(Max('date'))['date__max']
        start_date = latest_date - timedelta(days=30)
        
        self.stdout.write(f"Verifying data from {start_date} to {latest_date}")

        # 1. Count unique buy brokers in source
        total_buy_brokers = FloorsheetData.objects.filter(
            date__range=(start_date, latest_date)
        ).values('buyer').distinct().count()
        
        self.stdout.write(f"\n1. Total unique buy brokers in source: {total_buy_brokers}")

        # 2. Count processed brokers
        processed_count = BulkVolumeTrade.objects.filter(
            time_frame='1 Month'
        ).values('buy_broker').distinct().count()
        
        self.stdout.write(f"2. Processed buy brokers count: {processed_count}")

        # 3. Find missing brokers
        source_brokers = set(FloorsheetData.objects.filter(
            date__range=(start_date, latest_date)
        ).values_list('buyer', flat=True).distinct())
        
        processed_brokers = set(BulkVolumeTrade.objects.filter(
            time_frame='1 Month'
        ).values_list('buy_broker', flat=True))
        
        missing_brokers = source_brokers - processed_brokers
        
        self.stdout.write(f"\n3. Missing brokers count: {len(missing_brokers)}")
        if missing_brokers:
            self.stdout.write("Sample missing brokers: " + ", ".join(list(missing_brokers)[:5]))

        # 4. Verify top script selection for sample brokers
        some_brokers = FloorsheetData.objects.filter(
            date__range=(start_date, latest_date)
        ).values('buyer').annotate(count=Count('id')).order_by('-count')[:3]
        
        self.stdout.write("\n4. Verifying top script selection for sample brokers:")
        for broker in some_brokers:
            broker_name = broker['buyer']
            actual = FloorsheetData.objects.filter(
                date__range=(start_date, latest_date),
                buyer=broker_name
            ).values('symbol').annotate(
                total_quantity=Sum('quantity'),
                max_quantity=Max('quantity')
            ).order_by('-total_quantity').first()
            
            processed = BulkVolumeTrade.objects.filter(
                time_frame='1 Month',
                buy_broker=broker_name
            ).first()
            
            self.stdout.write(f"\nBroker: {broker_name}")
            self.stdout.write(f"Actual top: {actual['symbol']} (Total: {actual['total_quantity']}, Max: {actual['max_quantity']})")
            self.stdout.write(f"Processed: {processed.script if processed else 'Missing'} (Qty: {processed.quantity if processed else 'N/A'})")
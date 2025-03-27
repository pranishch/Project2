from django.core.management.base import BaseCommand
from django.db.models import Sum, Count
from data_analysis.models import FloorsheetData, IlliquidStockReport
import pandas as pd
from datetime import datetime, timedelta
import logging
import gc

class Command(BaseCommand):
    help = 'Analyze illiquid stocks with memory-efficient processing'

    def handle(self, *args, **kwargs):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        logger = logging.getLogger(__name__)
        logger.info("Starting optimized illiquid stocks analysis...")

        current_date = datetime.now().date()
        two_months_ago = current_date - timedelta(days=60)
        logger.info(f"Analyzing data from {two_months_ago} to {current_date}")

        try:
            # PHASE 1: Calculate two-month statistics per symbol
            logger.info("Calculating two-month statistics per symbol...")
            
            # Get total volume and trading days per symbol
            symbol_stats = FloorsheetData.objects.filter(
                date__gte=two_months_ago,
                date__lte=current_date
            ).values('symbol').annotate(
                total_volume=Sum('quantity'),
                trading_days=Count('date', distinct=True)
            )
            
            # Calculate average daily volume percentage and filter for illiquid stocks
            illiquid_symbols = []
            volume_dict = {}
            
            for stat in symbol_stats:
                symbol = stat['symbol']
                total_volume = stat['total_volume']
                trading_days = stat['trading_days'] or 1  # Avoid division by zero
                
                avg_daily_percent = (total_volume / trading_days) / total_volume * 100
                
                if avg_daily_percent < 10:  # Only consider stocks with <10% average daily volume
                    illiquid_symbols.append(symbol)
                    volume_dict[symbol] = total_volume
            
            if not illiquid_symbols:
                logger.info("No illiquid stocks found (avg daily volume <10%)")
                return
                
            logger.info(f"Found {len(illiquid_symbols)} illiquid stocks to monitor")
            
            # PHASE 2: Process data in date chunks to find unusual activity
            date_range = pd.date_range(start=two_months_ago, end=current_date)
            unusual_activities = []
            
            for day in date_range:
                day_date = day.date()
                logger.info(f"Processing {day_date}...")
                
                # Get daily data for illiquid symbols only
                chunk_size = 10000
                offset = 0
                daily_data = []
                
                while True:
                    chunk = list(FloorsheetData.objects.filter(
                        date=day_date,
                        symbol__in=illiquid_symbols
                    ).values(
                        'symbol', 'buyer', 'seller', 'quantity'
                    )[offset:offset + chunk_size])
                    
                    if not chunk:
                        break
                    
                    daily_data.extend(chunk)
                    offset += chunk_size
                
                if not daily_data:
                    continue
                
                # Create daily DataFrame
                daily_df = pd.DataFrame(daily_data)
                
                # Calculate daily volume per symbol
                daily_volume = daily_df.groupby('symbol')['quantity'].sum().reset_index()
                daily_volume.columns = ['symbol', 'daily_volume']
                
                # Calculate volume percentage
                daily_volume['volume_percent'] = daily_volume.apply(
                    lambda row: (row['daily_volume'] / volume_dict.get(row['symbol'], 1)) * 100,
                    axis=1
                )
                
                # Find symbols with >= 20% volume
                spike_symbols = daily_volume[daily_volume['volume_percent'] >= 20]
                
                # For each spike symbol, analyze broker activity
                for _, spike_row in spike_symbols.iterrows():
                    symbol = spike_row['symbol']
                    
                    # Filter trades for this symbol on this day
                    symbol_trades = daily_df[daily_df['symbol'] == symbol]
                    
                    # Get unique buyers/sellers
                    unique_buyers = symbol_trades['buyer'].nunique()
                    unique_sellers = symbol_trades['seller'].nunique()
                    
                    if unique_buyers <= 2 and unique_sellers <= 2:
                        dominant_buyer = symbol_trades.groupby('buyer')['quantity'].sum().idxmax()
                        dominant_seller = symbol_trades.groupby('seller')['quantity'].sum().idxmax()
                        
                        unusual_activities.append({
                            'symbol': symbol,
                            'date': day_date,
                            'volume_percent': spike_row['volume_percent'],
                            'buying_broker': dominant_buyer,
                            'selling_broker': dominant_seller,
                            'no_of_transactions': len(symbol_trades)
                        })
                
                # Clear memory
                del daily_df, daily_data, chunk
                gc.collect()
            
            # PHASE 3: Save reports
            if unusual_activities:
                logger.info(f"Saving {len(unusual_activities)} unusual activities...")
                for activity in unusual_activities:
                    IlliquidStockReport.objects.update_or_create(
                        script=activity['symbol'],
                        analysis_date=activity['date'],
                        defaults={
                            'no_of_transactions': activity['no_of_transactions'],
                            'volume_percent': activity['volume_percent'],
                            'avg_volume_percent': activity['volume_percent'],  # Using spike % as avg for this report
                            'buying_broker': activity['buying_broker'],
                            'selling_broker': activity['selling_broker'],
                        }
                    )
            
            logger.info("Analysis completed successfully")
            
        except Exception as e:
            logger.error(f"Error during analysis: {str(e)}", exc_info=True)
            raise
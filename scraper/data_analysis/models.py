from django.db import models

class FloorsheetData(models.Model):
    transaction_no = models.CharField(max_length=100,null=True,blank=True)
    symbol = models.CharField(max_length=255, null=True, blank=True)  # Stock symbol
    buyer = models.CharField(max_length=255, null=True, blank=True)   # Buyer broker
    seller = models.CharField(max_length=255, null=True, blank=True)  # Seller broker
    quantity = models.FloatField(null=True, blank=True)             # Quantity traded
    rate = models.FloatField(null=True, blank=True)                 # Rate per unit
    amount = models.FloatField(null=True, blank=True)               # Total amount
    date = models.DateField(null=True, blank=True)  # Keep it nullable
    
class BrokerTracker(models.Model):
    broker_id = models.CharField(max_length=100, null=True, blank=True)
    date = models.DateField(null=True, blank=True)  # Keep it nullable
    total_volume = models.FloatField(null=True, blank=True)
    time_period = models.CharField(max_length=50, default='latest_date', null=True, blank=True)  # Add default value

class StockwiseBroker(models.Model):
    stock_name = models.CharField(max_length=10, null=True, blank=True)
    broker_id = models.CharField(max_length=50, null=True, blank=True)
    volume = models.IntegerField(null=True, blank=True)
    percent_volume = models.FloatField(null=True, blank=True)
    date_range = models.CharField(max_length=50, null=True, blank=True)  # Add a default value
    time_frame = models.CharField(max_length=20, null=True, blank=True)  

class BulkVolumeTrade(models.Model):
    script = models.CharField(max_length=255,null=True, blank=True)  # Stock symbol
    buy_broker = models.CharField(max_length=255, null=True, blank=True)  # Buyer broker
    quantity = models.FloatField(null=True, blank=True)  # Total quantity
    time_frame = models.CharField(max_length=20, null=True, blank=True)
    date_range = models.CharField(max_length=50, null=True, blank=True)  # Add a default value

class WashTrade(models.Model):
    script = models.CharField(max_length=255, null=True, blank=True)
    buyer_seller = models.CharField(max_length=255, null=True, blank=True)  # Broker who is both buyer and seller
    quantity = models.FloatField(null=True, blank=True)
    date_range = models.CharField(max_length=50, null=True, blank=True)  # Stores the time range string
    time_frame = models.CharField(max_length=20, null=True, blank=True)
        
class BigPlayerAccumulation(models.Model):
    script = models.CharField(max_length=255, null=True, blank=True)
    quantity = models.FloatField(null=True, blank=True)
    buying_broker = models.CharField(max_length=255, null=True, blank=True)
    selling_brokers = models.TextField(null=True, blank=True)  # Stores multiple sellers as JSON
    time_frame = models.CharField(max_length=20, null=True, blank=True)
    date_range = models.CharField(max_length=50, null=True, blank=True)
            
class BigPlayerDistribution(models.Model):
    script = models.CharField(max_length=255, null=True, blank=True)
    quantity = models.FloatField(null=True, blank=True)
    buying_brokers = models.TextField(null=True, blank=True)  # Changed to TextField for JSON
    selling_broker = models.CharField(max_length=255, null=True, blank=True)  # Changed from TextField
    time_frame = models.CharField(max_length=20, null=True, blank=True)
    date_range = models.CharField(max_length=50, null=True, blank=True)
        
class BrokerWiseAccumulation(models.Model):
    broker = models.CharField(max_length=255, null=True, blank=True)  # Broker number/name
    script = models.CharField(max_length=255, null=True, blank=True)  # Stock symbol
    quantity = models.FloatField(null=True, blank=True)  # Total buying quantity
    date_range = models.CharField(max_length=255, null=True, blank=True)  # Date range of accumulation
    time_frame = models.CharField(max_length=50, null=True, blank=True)  # Time frame (daily, weekly, etc.)

class BrokerWiseDistribution(models.Model):
    broker = models.CharField(max_length=255, null=True, blank=True)  # Broker number/name
    script = models.CharField(max_length=255, null=True, blank=True)  # Stock symbol
    selling_quantity = models.FloatField(null=True, blank=True)  # Total buying quantity
    date_range = models.CharField(max_length=255, null=True, blank=True)  # Date range of accumulation
    time_frame = models.CharField(max_length=50, null=True, blank=True)  # Time frame (daily, weekly, etc.)

class PingPongTrade(models.Model):
    broker_a = models.CharField(max_length=255)
    broker_b = models.CharField(max_length=255)
    symbol = models.CharField(max_length=255, null=True)
    occurrences = models.IntegerField()
    total_quantity = models.FloatField(default=0)
    total_amount = models.FloatField(default=0)
    detection_date = models.DateTimeField(auto_now_add=True)
    is_investigated = models.BooleanField(default=False)
    trade_details = models.JSONField(null=True)
    
    # New fields for time window tracking
    min_time_diff = models.FloatField(null=True, help_text="Minimum time difference between trades (minutes)")
    max_time_diff = models.FloatField(null=True, help_text="Maximum time difference between trades (minutes)")

    class Meta:
        unique_together = ('broker_a', 'broker_b', 'symbol')
        verbose_name_plural = 'Ping Pong Trades'
        indexes = [
            models.Index(fields=['broker_a', 'broker_b']),
            models.Index(fields=['detection_date']),
        ]


class FlatPriceDetection(models.Model):
    symbol = models.CharField(max_length=50, null=True, blank=True)  # Remove null/blank for required field
    total_transactions = models.IntegerField(null=True, blank=True)  # First make it optional
    price_change = models.FloatField(null=True, blank=True)  # Remove null/blank
    date_range = models.CharField(max_length=100, null=True, blank=True)  # Remove null/blank
    
class WallBreakTracking(models.Model):
    script_name = models.CharField(max_length=255, null=True, blank=True)
    resistance = models.FloatField(null=True, blank=True)
    price = models.FloatField(null=True, blank=True)
    remarks = models.CharField(max_length=255, null=True, blank=True)
    date = models.DateField(null=True, blank=True)
    
    class Meta:
        verbose_name = "Wall Break Tracking"
        verbose_name_plural = "Wall Break Trackings"
        ordering = ['-date', 'script_name']
        
    def __str__(self):
        return f"{self.script_name} - {self.date}"   

    
class AccumulationData(models.Model):
    symbol = models.CharField(max_length=255, null=True, blank=True)
    date = models.DateField(null=True, blank=True)
    time_frame = models.CharField(max_length=20, null=True, blank=True)  # Increased length for full frame names
    avg_price = models.FloatField(null=True, blank=True)
    total_volume = models.FloatField(null=True, blank=True)
    remarks = models.TextField(null=True, blank=True)
    date_range = models.CharField(max_length=50, null=True, blank=True)
    
    class Meta:
        verbose_name = "Accumulation Data"
        verbose_name_plural = "Accumulation Data"
        ordering = ['-date', 'symbol', 'time_frame']
        indexes = [
            models.Index(fields=['symbol']),
            models.Index(fields=['date']),
            models.Index(fields=['time_frame']),
        ]
        
    def __str__(self):
        return f"{self.symbol} - {self.time_frame} - {self.date_range}"    


class DistributionAnalysis(models.Model):
    timestamp = models.DateField(null=True, blank=True)
    avg_price = models.FloatField(null=True, blank=True)
    total_volume = models.FloatField(null=True, blank=True)

    class Meta:
        db_table = 'distribution_analysis'

class LargeSellers(models.Model):
    symbol = models.CharField(max_length=255, null=True, blank=True)
    price = models.FloatField(null=True, blank=True)
    volume = models.FloatField(null=True, blank=True)
    timestamp = models.DateTimeField(null=True, blank=True)

    class Meta:
        db_table = 'large_sellers'

class IlliquidStockReport(models.Model):
    script = models.CharField(max_length=255, null=True, blank=True)
    no_of_transactions = models.IntegerField(null=True, blank=True)
    volume_percent = models.FloatField(null=True, blank=True)  # Daily spike percentage
    avg_volume_percent = models.FloatField(null=True, blank=True)  # Average daily volume percentage
    buying_broker = models.CharField(max_length=255, null=True, blank=True)
    selling_broker = models.CharField(max_length=255, null=True, blank=True)
    analysis_date = models.DateField(null=True, blank=True)

    class Meta:
        unique_together = ('script', 'analysis_date')
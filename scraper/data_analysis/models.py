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

class FunOwnership(models.Model):
    script_id = models.CharField(max_length=20, null=True, blank=True)
    updated_on = models.DateField(null=True, blank=True)
    promoter_shares = models.BigIntegerField(null=True, blank=True)
    public_shares = models.BigIntegerField(null=True, blank=True)
    total_listed_shares = models.BigIntegerField(null=True, blank=True)
    symbol = models.CharField(max_length=20, null=True, blank=True)

    class Meta:
        verbose_name = "FUN Ownership"
        verbose_name_plural = "FUN Ownership Data"
        ordering = ['-updated_on', 'symbol']
        unique_together = ('script_id', 'updated_on')

    def __str__(self):
        return f"{self.symbol} - {self.updated_on}"
    
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

class BulkVolume(models.Model):
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
    
class WallBreak(models.Model):
    script_name = models.CharField(max_length=255, null=True, blank=True)
    resistance = models.FloatField(null=True, blank=True)
    price = models.FloatField(null=True, blank=True)
    remarks = models.CharField(max_length=255, null=True, blank=True)
    date = models.DateField(null=True, blank=True)
    
    class Meta:
        verbose_name = "Wall Break Tracking"
        verbose_name_plural = "Wall Break Trackings"
        ordering = ['-date', 'script_name']
 
class Accumulation(models.Model):
    symbol = models.CharField(max_length=255, null=True, blank=True)
    date = models.DateField(null=True, blank=True)
    time_frame = models.CharField(max_length=20, null=True, blank=True)  # Increased length for full frame names
    avg_price = models.FloatField(null=True, blank=True)
    total_volume = models.FloatField(null=True, blank=True)
    remarks = models.TextField(null=True, blank=True)
    date_range = models.CharField(max_length=50, null=True, blank=True)
 

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

class IlliquidStock(models.Model):
    script = models.CharField(max_length=255, null=True, blank=True)
    no_of_transactions = models.IntegerField(null=True, blank=True)
    volume_percent = models.FloatField(null=True, blank=True)  # Daily spike percentage
    avg_volume_percent = models.FloatField(null=True, blank=True)  # Average daily volume percentage
    buying_broker = models.CharField(max_length=255, null=True, blank=True)
    selling_broker = models.CharField(max_length=255, null=True, blank=True)
    analysis_date = models.DateField(null=True, blank=True)

    class Meta:
        unique_together = ('script', 'analysis_date')


class VolumeTrade(models.Model):
    time_frame = models.CharField(max_length=10, null=True, blank=True)  # No choices, just free text
    symbol = models.CharField(max_length=20, null=True, blank=True)
    public_share = models.BigIntegerField(null=True, blank=True)
    trading_volume = models.BigIntegerField(null=True, blank=True)
    traded_volume_percent = models.DecimalField(max_digits=10, decimal_places=2, null=True, blank=True)
    report_date = models.DateField(auto_now_add=True, null=True, blank=True)
  
class ConsecutiveStreak(models.Model):
    STREAK_TYPES = [
        ('buying', 'Buying Streak'),
        ('selling', 'Selling Streak'),
    ]
    
    symbol = models.CharField(max_length=255, null=True, blank=True)
    broker = models.CharField(max_length=255, null=True, blank=True)
    streak_type = models.CharField(max_length=10, choices=STREAK_TYPES, null=True, blank=True)
    start_date = models.DateField(null=True, blank=True)
    end_date = models.DateField(null=True, blank=True)
    streak_length = models.PositiveIntegerField(null=True, blank=True)
    total_quantity = models.FloatField(null=True, blank=True)
  
class JumpFallDetection(models.Model):
    MOVEMENT_TYPES = [
        ('jump', 'Price Jump'),
        ('fall', 'Price Fall'),
    ]
    
    symbol = models.CharField(max_length=255, null=True, blank=True)
    date = models.DateField(null=True, blank=True)
    movement_type = models.CharField(max_length=4, choices=MOVEMENT_TYPES, null=True, blank=True)
    percentage_change = models.FloatField(null=True, blank=True)
    open_price = models.FloatField(null=True, blank=True)
    close_price = models.FloatField(null=True, blank=True)
    low_price = models.FloatField(null=True, blank=True)
    high_price = models.FloatField(null=True, blank=True)
    detected_at = models.DateTimeField(auto_now_add=True)


class PriceVolumeCorrelation(models.Model):
    MOVEMENT_TYPES = [
        ('strong_rise', 'Strong Rise (Price↑, Volume↑)'),
        ('weak_rise', 'Weak Rise (Price↑, Volume↓)'),
        ('strong_fall', 'Strong Fall (Price↓, Volume↑)'),
        ('weak_fall', 'Weak Fall (Price↓, Volume↓)'),
        ('neutral', 'Neutral')
    ]
    
    symbol = models.CharField(max_length=255, null=True, blank=True)
    date = models.DateField(null=True, blank=True)
    movement_type = models.CharField(max_length=11, choices=MOVEMENT_TYPES, null=True, blank=True)
    price_change = models.FloatField(help_text="Percentage change", null=True, blank=True)
    volume_ratio = models.FloatField(help_text="Volume compared to 3-day average", null=True, blank=True)
    price = models.FloatField(help_text="Average price for the day", null=True, blank=True)
    volume = models.FloatField(help_text="Total volume for the day", null=True, blank=True)
    analyzed_at = models.DateTimeField(auto_now_add=True)


class WeekendEffect(models.Model):
    ANALYSIS_TYPES = (
        ('DAY_OF_WEEK', 'Day of Week Returns'),
        ('WEEKEND_EFFECT', 'Weekend Effect'),
        ('WEEKLY_STATS', 'Weekly Statistics'),
    )
    
    analysis_type = models.CharField(max_length=20, choices=ANALYSIS_TYPES)
    symbol = models.CharField(max_length=255, null=True, blank=True)
    day_of_week = models.CharField(max_length=10, null=True, blank=True)
    week_start = models.DateField(null=True, blank=True)
    week_end = models.DateField(null=True, blank=True)
    open_price = models.FloatField(null=True, blank=True)
    close_price = models.FloatField(null=True, blank=True)
    return_value = models.FloatField(null=True, blank=True)
    description = models.TextField(null=True, blank=True)
    analysis_date = models.DateTimeField()
    
class ContinuousLowVolume(models.Model):
    symbol = models.CharField(max_length=255, null=True, blank=True)
    start_date = models.DateField(null=True, blank=True)
    end_date = models.DateField(null=True, blank=True)
    consecutive_days = models.PositiveIntegerField(null=True, blank=True)
    avg_volume = models.FloatField(null=True, blank=True)
    max_volume_threshold = models.PositiveIntegerField(null=True, blank=True)
    detected_at = models.DateTimeField(auto_now_add=True)

class SingleDayHighVolume(models.Model):
    symbol = models.CharField(max_length=255, null=True, blank=True)
    alert_date = models.DateField(null=True, blank=True)
    volume = models.FloatField(help_text="Actual trading volume on alert day", null=True, blank=True)
    avg_volume = models.FloatField(help_text="Average volume from lookback period", null=True, blank=True)
    volume_ratio = models.FloatField(help_text="Volume to average volume ratio", null=True, blank=True)
    multiplier_threshold = models.FloatField(help_text="Threshold multiplier used", null=True, blank=True)
    detected_at = models.DateTimeField(auto_now_add=True)

class ConsecutiveUpDown(models.Model):
    MOVEMENT_TYPES = [
        ('UP', 'Price Increasing'),
        ('DOWN', 'Price Decreasing')
    ]
    
    symbol = models.CharField(max_length=255, null=True, blank=True)
    movement_type = models.CharField(max_length=4, choices=MOVEMENT_TYPES, null=True, blank=True)
    start_date = models.DateField(null=True, blank=True)
    end_date = models.DateField(null=True, blank=True)
    days_count = models.PositiveIntegerField(null=True, blank=True)
    start_price = models.FloatField(null=True, blank=True)
    end_price = models.FloatField(null=True, blank=True)
    percent_change = models.FloatField(null=True, blank=True)
    detected_at = models.DateTimeField(auto_now_add=True)

class VolumeSpikeDetection(models.Model):
    symbol = models.CharField(max_length=255, null=True, blank=True)
    volume = models.FloatField(null=True, blank=True)
    avg_volume_percent = models.FloatField(null=True, blank=True)
    date = models.DateField(null=True, blank=True)

class BrokerPair(models.Model):
    date = models.DateField(null=True, blank=True)
    quantity = models.FloatField(null=True, blank=True)
    transaction_count = models.IntegerField(null=True, blank=True)
    buyer_broker = models.CharField(max_length=255, null=True, blank=True)
    seller_broker = models.CharField(max_length=255, null=True, blank=True)

class BrokerTransfer(models.Model):
    transaction_id = models.CharField(max_length=100, unique=True, null=True, blank=True)
    symbol = models.CharField(max_length=20, null=True, blank=True)
    selling_broker = models.CharField(max_length=20, null=True, blank=True)
    buying_broker = models.CharField(max_length=20, null=True, blank=True)
    kitta = models.IntegerField(null=True, blank=True)
    first_date = models.DateField(null=True, blank=True)
    last_date = models.DateField(null=True, blank=True)
    transaction_count = models.IntegerField(null=True, blank=True)
    days_active = models.IntegerField(null=True, blank=True)
    
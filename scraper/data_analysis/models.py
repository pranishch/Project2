from django.db import models

class FloorsheetData(models.Model):
    symbol = models.CharField(max_length=255)  # Stock symbol
    buyer = models.CharField(max_length=255)   # Buyer broker
    seller = models.CharField(max_length=255)  # Seller broker
    quantity = models.FloatField()             # Quantity traded
    rate = models.FloatField()                 # Rate per unit
    amount = models.FloatField()               # Total amount
    date = models.DateField(null=True, blank=True)  # Keep it nullable
    
class BrokerTracker(models.Model):
    broker_id = models.CharField(max_length=100)
    date = models.DateField(null=True, blank=True)  # Keep it nullable
    total_volume = models.FloatField()
    time_period = models.CharField(max_length=50, default='latest_date')  # Add default value

class StockwiseBroker(models.Model):
    stock_name = models.CharField(max_length=10)
    broker_id = models.CharField(max_length=50)
    volume = models.IntegerField()
    percent_volume = models.FloatField()
    date_range = models.CharField(max_length=50)  # Add a default value
    time_frame = models.CharField(max_length=20)  

class BulkVolumeTrade(models.Model):
    script = models.CharField(max_length=255)  # Stock symbol
    buy_broker = models.CharField(max_length=255)  # Buyer broker
    quantity = models.FloatField()  # Total quantity
    time_frame = models.CharField(max_length=20)
    date_range = models.CharField(max_length=50)  # Add a default value

class WashTrade(models.Model):
    script = models.CharField(max_length=255)
    quantity = models.FloatField()
    date_range = models.CharField(max_length=50)  # Stores the time range string
    time_frame = models.CharField(max_length=20)
    buyer_seller = models.CharField(max_length=255)  # Broker who is both buyer and seller
        
class BigPlayerAccumulation(models.Model):
    script = models.CharField(max_length=255)
    quantity = models.FloatField()
    buying_broker = models.CharField(max_length=255)
    selling_brokers = models.TextField()  # Stores multiple sellers as JSON
    time_frame = models.CharField(max_length=20)
    date_range = models.CharField(max_length=50)
            
 
        
   
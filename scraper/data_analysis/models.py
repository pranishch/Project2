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

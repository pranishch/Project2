from django.db import models

class StockData(models.Model):
    serial_number = models.IntegerField()  # SN (Serial Number)
    symbol = models.CharField(max_length=50, unique=False)  # Stock Symbol
    close_price = models.CharField(max_length=100,null=True, blank=True)  # Close Price (Rs)
    open_price = models.CharField(max_length=100,null=True, blank=True)  # Open Price (Rs)
    high_price = models.CharField(max_length=100,null=True, blank=True)  # High Price (Rs)
    low_price = models.CharField(max_length=100,null=True, blank=True)  # Low Price (Rs)
    total_traded_quantity = models.CharField(max_length=100,null=True, blank=True)  # Total Traded Quantity
    total_traded_value = models.CharField(max_length=100, null=True, blank=True)  # Total Traded Value (Rs)
    total_trades = models.CharField(max_length=100,null=True, blank=True)  # Total Trades
    last_traded_price = models.CharField(max_length=100,null=True, blank=True)  # LTP (Last Traded Price)
    previous_close_price = models.CharField(max_length=100,null=True, blank=True)  # Previous Day Close Price (Rs)
    average_traded_price = models.CharField(max_length=100,null=True, blank=True)  # Average Traded Price (Rs)
    week_52_high = models.CharField(max_length=100,null=True, blank=True)  # 52 Week High (Rs)
    week_52_low = models.CharField(max_length=100,null=True, blank=True)  # 52 Week Low (Rs)
    market_capitalization = models.CharField(max_length=100, null=True, blank=True)  # Market Cap (Rs in Millions)
    timestamp = models.DateTimeField(auto_now_add=True)  # Auto-save timestamp on entry creation

    def __str__(self):
        return f"{self.symbol} - {self.close_price}"

class StockTransaction(models.Model):
    SN = models.CharField(max_length=100)
    contract_no = models.CharField(max_length=100)  # Adjust field name
    stock_symbol = models.CharField(max_length=50)  # Adjust field name
    buyer = models.CharField(max_length=100)  # Adjust field name
    seller = models.CharField(max_length=100)  # Adjust field name
    quantity = models.IntegerField()  # Adjust field type if necessary
    rate = models.DecimalField(max_digits=10, decimal_places=2)  # Adjust field type
    amount = models.DecimalField(max_digits=15, decimal_places=2)  # Adjust field type

    def __str__(self):
        return f"Transaction {self.contract_no} - {self.stock_symbol}"
    

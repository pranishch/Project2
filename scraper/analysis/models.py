from django.db import models

class BrokerVolume(models.Model):
    broker_id = models.CharField(max_length=100)
    date = models.DateField()
    total_volume = models.FloatField()
    time_period = models.CharField(max_length=50, default='latest_date')  # Add default value

    def __str__(self):
        return f"{self.broker_id} - {self.date} - {self.time_period}"
class BrokerData(models.Model):
    stock_name = models.CharField(max_length=10)
    broker_id = models.CharField(max_length=50)
    volume = models.IntegerField()
    percent_volume = models.FloatField()
    date = models.DateField()

    def __str__(self):
        return f"{self.stock_name} - {self.broker_id}"
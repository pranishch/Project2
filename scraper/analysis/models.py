from django.db import models

class BrokerVolume(models.Model):
    broker_id = models.CharField(max_length=100)
    date = models.DateField()
    total_volume = models.FloatField()
    time_period = models.CharField(max_length=50, default='latest_date')  # Add default value

    def __str__(self):
        return f"{self.broker_id} - {self.date} - {self.time_period}"
# Generated by Django 5.1.6 on 2025-03-30 11:25

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('data_analysis', '0093_tradinghalt'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='tradinghalt',
            unique_together={('symbol', 'date', 'rate', 'open_price', 'price_change_pct')},
        ),
    ]

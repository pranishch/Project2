# Generated by Django 5.1.6 on 2025-03-31 07:40

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_analysis', '0110_stockmovementreport'),
    ]

    operations = [
        migrations.CreateModel(
            name='StockReboundReport',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('symbol', models.CharField(blank=True, max_length=255, null=True)),
                ('rebound_date', models.DateField(blank=True, null=True)),
                ('consecutive_drops', models.IntegerField(blank=True, null=True)),
                ('drop_percentage', models.FloatField(blank=True, null=True)),
                ('rebound_percentage', models.FloatField(blank=True, null=True)),
                ('volume_change', models.FloatField(blank=True, null=True)),
            ],
        ),
    ]

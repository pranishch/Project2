# Generated by Django 5.1.6 on 2025-03-31 08:32

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_analysis', '0113_brokerdominance'),
    ]

    operations = [
        migrations.CreateModel(
            name='Stock52WeekExtremes',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('symbol', models.CharField(blank=True, max_length=10, null=True)),
                ('date', models.DateField(blank=True, null=True)),
                ('latest_price', models.DecimalField(blank=True, decimal_places=2, max_digits=15, null=True)),
                ('week_52_high', models.DecimalField(blank=True, decimal_places=2, max_digits=15, null=True)),
                ('week_52_low', models.DecimalField(blank=True, decimal_places=2, max_digits=15, null=True)),
                ('status', models.CharField(blank=True, max_length=10, null=True)),
            ],
        ),
    ]

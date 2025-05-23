# Generated by Django 5.1.6 on 2025-03-25 10:37

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_analysis', '0011_rename_quantity_brokerwiseaccumulation_buying_quantity'),
    ]

    operations = [
        migrations.CreateModel(
            name='BrokerWiseDistribution',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('broker', models.CharField(blank=True, max_length=255, null=True)),
                ('script', models.CharField(blank=True, max_length=255, null=True)),
                ('selling_quantity', models.FloatField(blank=True, null=True)),
                ('date_range', models.CharField(blank=True, max_length=255, null=True)),
                ('time_frame', models.CharField(blank=True, max_length=50, null=True)),
            ],
        ),
    ]

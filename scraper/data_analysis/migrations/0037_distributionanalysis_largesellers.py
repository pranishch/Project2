# Generated by Django 5.1.6 on 2025-03-26 10:12

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_analysis', '0036_delete_distributiondata'),
    ]

    operations = [
        migrations.CreateModel(
            name='DistributionAnalysis',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('timestamp', models.DateField()),
                ('avg_price', models.FloatField()),
                ('total_volume', models.FloatField()),
            ],
            options={
                'db_table': 'distribution_analysis',
            },
        ),
        migrations.CreateModel(
            name='LargeSellers',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('symbol', models.CharField(max_length=255)),
                ('price', models.FloatField()),
                ('volume', models.FloatField()),
                ('timestamp', models.DateTimeField()),
            ],
            options={
                'db_table': 'large_sellers',
            },
        ),
    ]

# Generated by Django 5.1.6 on 2025-03-25 16:02

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_analysis', '0017_delete_flatpricedetection'),
    ]

    operations = [
        migrations.CreateModel(
            name='FlatPriceDetection',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('symbol', models.CharField(max_length=255)),
                ('date_range', models.CharField(blank=True, max_length=255, null=True)),
                ('total_transactions', models.IntegerField()),
                ('flat_transactions', models.IntegerField()),
                ('avg_rate', models.FloatField()),
                ('min_rate', models.FloatField()),
                ('max_rate', models.FloatField()),
                ('price_change_percentage', models.FloatField()),
                ('created_at', models.DateTimeField(auto_now_add=True)),
            ],
            options={
                'ordering': ['-created_at'],
                'unique_together': {('symbol', 'date_range', 'created_at')},
            },
        ),
    ]

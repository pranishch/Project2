# Generated by Django 5.1.6 on 2025-03-27 11:56

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_analysis', '0063_rename_pricemovement_jumpfall_and_more'),
    ]

    operations = [
        migrations.CreateModel(
            name='PriceVolumeCorrelation',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('symbol', models.CharField(blank=True, max_length=255, null=True)),
                ('date', models.DateField(blank=True, null=True)),
                ('movement_type', models.CharField(blank=True, choices=[('strong_rise', 'Strong Rise (Price↑, Volume↑)'), ('weak_rise', 'Weak Rise (Price↑, Volume↓)'), ('strong_fall', 'Strong Fall (Price↓, Volume↑)'), ('weak_fall', 'Weak Fall (Price↓, Volume↓)'), ('neutral', 'Neutral')], max_length=11, null=True)),
                ('price_change', models.FloatField(blank=True, help_text='Percentage change', null=True)),
                ('volume_ratio', models.FloatField(blank=True, help_text='Volume compared to 3-day average', null=True)),
                ('price', models.FloatField(blank=True, help_text='Average price for the day', null=True)),
                ('volume', models.FloatField(blank=True, help_text='Total volume for the day', null=True)),
                ('analyzed_at', models.DateTimeField(auto_now_add=True)),
            ],
            options={
                'ordering': ['-date', 'symbol'],
                'indexes': [models.Index(fields=['symbol'], name='data_analys_symbol_0ae244_idx'), models.Index(fields=['date'], name='data_analys_date_cea297_idx'), models.Index(fields=['movement_type'], name='data_analys_movemen_2b1c6f_idx')],
            },
        ),
    ]

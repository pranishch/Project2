# Generated by Django 5.1.6 on 2025-03-26 07:23

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_analysis', '0027_alter_wallbreaktracking_date_and_more'),
    ]

    operations = [
        migrations.CreateModel(
            name='AccumulationData',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('symbol', models.CharField(blank=True, max_length=255, null=True)),
                ('date', models.DateField(blank=True, null=True)),
                ('time_frame', models.CharField(blank=True, max_length=20, null=True)),
                ('avg_price', models.FloatField(blank=True, null=True)),
                ('total_volume', models.FloatField(blank=True, null=True)),
                ('remarks', models.TextField(blank=True, null=True)),
                ('date_range', models.CharField(blank=True, max_length=50, null=True)),
            ],
            options={
                'verbose_name': 'Accumulation Data',
                'verbose_name_plural': 'Accumulation Data',
                'ordering': ['-date', 'symbol', 'time_frame'],
                'indexes': [models.Index(fields=['symbol'], name='data_analys_symbol_3a1515_idx'), models.Index(fields=['date'], name='data_analys_date_1e8101_idx'), models.Index(fields=['time_frame'], name='data_analys_time_fr_da426c_idx')],
            },
        ),
    ]

# Generated by Django 5.1.6 on 2025-03-25 11:27

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_analysis', '0013_rename_buying_quantity_brokerwiseaccumulation_quantity'),
    ]

    operations = [
        migrations.CreateModel(
            name='PingPongTrade',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('broker_a', models.CharField(max_length=255)),
                ('broker_b', models.CharField(max_length=255)),
                ('symbol', models.CharField(max_length=255, null=True)),
                ('occurrences', models.IntegerField()),
                ('total_quantity', models.FloatField(default=0)),
                ('total_amount', models.FloatField(default=0)),
                ('detection_date', models.DateTimeField(auto_now_add=True)),
                ('is_investigated', models.BooleanField(default=False)),
                ('trade_details', models.JSONField(null=True)),
            ],
            options={
                'verbose_name_plural': 'Ping Pong Trades',
                'indexes': [models.Index(fields=['broker_a', 'broker_b'], name='data_analys_broker__daedf6_idx'), models.Index(fields=['detection_date'], name='data_analys_detecti_3faf5d_idx')],
                'unique_together': {('broker_a', 'broker_b', 'symbol')},
            },
        ),
    ]

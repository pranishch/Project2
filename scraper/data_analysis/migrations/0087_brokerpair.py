# Generated by Django 5.1.6 on 2025-03-30 07:27

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_analysis', '0086_rename_weekendeffectresult_weekendeffect'),
    ]

    operations = [
        migrations.CreateModel(
            name='BrokerPair',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('date', models.DateField(blank=True, null=True)),
                ('quantity', models.FloatField(blank=True, null=True)),
                ('transaction_count', models.IntegerField(blank=True, null=True)),
                ('buyer_broker', models.CharField(blank=True, max_length=255, null=True)),
                ('seller_broker', models.CharField(blank=True, max_length=255, null=True)),
            ],
        ),
    ]

# Generated by Django 5.1.6 on 2025-03-29 06:54

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('data_analysis', '0070_alter_continuouslowvolume_options'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='pricevolumecorrelation',
            options={},
        ),
        migrations.RemoveIndex(
            model_name='pricevolumecorrelation',
            name='data_analys_symbol_0ae244_idx',
        ),
        migrations.RemoveIndex(
            model_name='pricevolumecorrelation',
            name='data_analys_date_cea297_idx',
        ),
        migrations.RemoveIndex(
            model_name='pricevolumecorrelation',
            name='data_analys_movemen_2b1c6f_idx',
        ),
    ]

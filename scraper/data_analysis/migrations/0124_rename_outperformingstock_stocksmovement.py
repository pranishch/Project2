# Generated by Django 5.1.6 on 2025-03-31 11:45

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('data_analysis', '0123_delete_stockmovementreport'),
    ]

    operations = [
        migrations.RenameModel(
            old_name='OutperformingStock',
            new_name='StocksMovement',
        ),
    ]

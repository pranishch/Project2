# Generated by Django 5.1.6 on 2025-03-07 05:50

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('scrap', '0006_daywiselivedata'),
    ]

    operations = [
        migrations.DeleteModel(
            name='DaywiseLiveData',
        ),
        migrations.RemoveField(
            model_name='stocktransaction',
            name='timestamp',
        ),
    ]

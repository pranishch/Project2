# Generated by Django 5.1.6 on 2025-03-25 10:40

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('data_analysis', '0012_brokerwisedistribution'),
    ]

    operations = [
        migrations.RenameField(
            model_name='brokerwiseaccumulation',
            old_name='buying_quantity',
            new_name='quantity',
        ),
    ]

# Generated by Django 5.1.6 on 2025-03-25 10:10

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('data_analysis', '0009_brokerwiseaccumulation'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='brokerwiseaccumulation',
            name='created_at',
        ),
    ]

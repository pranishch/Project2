# Generated by Django 5.1.6 on 2025-03-26 06:50

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_analysis', '0022_remove_flatpricedetection_date_and_more'),
    ]

    operations = [
        migrations.CreateModel(
            name='WallBreakTracking',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('script_name', models.CharField(max_length=255)),
                ('resistance', models.FloatField()),
                ('price', models.FloatField()),
                ('remarks', models.CharField(max_length=255)),
                ('date', models.DateField()),
            ],
            options={
                'verbose_name': 'Wall Break Tracking',
                'verbose_name_plural': 'Wall Break Trackings',
                'ordering': ['-date', 'script_name'],
            },
        ),
    ]

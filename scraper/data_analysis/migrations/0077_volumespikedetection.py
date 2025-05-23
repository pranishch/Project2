# Generated by Django 5.1.6 on 2025-03-30 06:51

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_analysis', '0076_continuouslowvolume_avg_volume'),
    ]

    operations = [
        migrations.CreateModel(
            name='VolumeSpikeDetection',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('symbol', models.CharField(blank=True, max_length=255, null=True)),
                ('volume', models.FloatField(blank=True, null=True)),
                ('avg_volume_percent', models.FloatField(blank=True, null=True)),
                ('date', models.DateField(blank=True, null=True)),
            ],
        ),
    ]

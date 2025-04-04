# Generated by Django 5.1.6 on 2025-03-30 10:48

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_analysis', '0088_brokertransfer'),
    ]

    operations = [
        migrations.CreateModel(
            name='ConsecutiveNoTrade',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('symbol', models.CharField(blank=True, max_length=255, null=True)),
                ('start_date', models.DateField(blank=True, null=True)),
                ('end_date', models.DateField(blank=True, null=True)),
                ('days_count', models.PositiveIntegerField(blank=True, null=True)),
                ('calculated_date', models.DateField(blank=True, null=True)),
            ],
        ),
    ]

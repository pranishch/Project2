# Generated by Django 5.1.6 on 2025-03-26 06:57

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_analysis', '0026_wallbreaktracking'),
    ]

    operations = [
        migrations.AlterField(
            model_name='wallbreaktracking',
            name='date',
            field=models.DateField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='wallbreaktracking',
            name='price',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='wallbreaktracking',
            name='remarks',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
        migrations.AlterField(
            model_name='wallbreaktracking',
            name='resistance',
            field=models.FloatField(blank=True, null=True),
        ),
        migrations.AlterField(
            model_name='wallbreaktracking',
            name='script_name',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]

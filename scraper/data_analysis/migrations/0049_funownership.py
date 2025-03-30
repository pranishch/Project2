# Generated by Django 5.1.6 on 2025-03-27 08:18

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('data_analysis', '0048_delete_funownership'),
    ]

    operations = [
        migrations.CreateModel(
            name='FunOwnership',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('script_id', models.CharField(blank=True, max_length=20, null=True)),
                ('updated_on', models.DateField(blank=True, null=True)),
                ('promoter_shares', models.BigIntegerField(blank=True, null=True)),
                ('public_shares', models.BigIntegerField(blank=True, null=True)),
                ('total_listed_shares', models.BigIntegerField(blank=True, null=True)),
                ('symbol', models.CharField(blank=True, max_length=20, null=True)),
            ],
            options={
                'verbose_name': 'FUN Ownership',
                'verbose_name_plural': 'FUN Ownership Data',
                'ordering': ['-updated_on', 'symbol'],
                'unique_together': {('script_id', 'updated_on')},
            },
        ),
    ]

from django.core.management.base import BaseCommand
from data_analysis.models import FloorsheetData, BrokerTracker
from datetime import datetime, timedelta
import pandas as pd

class Command(BaseCommand):
    help = 'Process and store broker volume data from FloorsheetData using Django ORM'

    def handle(self, *args, **kwargs):
        # Clear existing data in the BrokerVolume table
        self.clear_existing_data()
        BrokerTracker.objects.all().delete()

        # Process and store data
        self.process_and_store_data()
        self.stdout.write(self.style.SUCCESS('Successfully processed and stored data from FloorsheetData.'))

    def clear_existing_data(self):
        """Clear all existing data in the BrokerVolume table."""
        BrokerTracker.objects.all().delete()
        self.stdout.write(self.style.SUCCESS('Successfully cleared existing data in the BrokerVolume table.'))

    def process_and_store_data(self):
        """Process the FloorsheetData and store it in the BrokerVolume table."""
        # Query FloorsheetData to get all records
        floorsheet_data = FloorsheetData.objects.all()

        # Convert queryset to a Pandas DataFrame
        df = pd.DataFrame(list(floorsheet_data.values('buyer', 'seller', 'quantity', 'date')))

        # Ensure required columns exist
        if not all(col in df.columns for col in ["buyer", "seller", "quantity", "date"]):
            raise KeyError("Missing required column(s) in data.")

        # Convert 'quantity' to numeric to prevent concatenation issues
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')

        # Convert date column to datetime and drop duplicates
        df['date'] = pd.to_datetime(df['date']).dt.date
        df.drop_duplicates(inplace=True)

        # Calculate buyer and seller volumes
        buyer_volume = df.groupby(['buyer', 'date'])['quantity'].sum().reset_index()
        seller_volume = df.groupby(['seller', 'date'])['quantity'].sum().reset_index()

        # Rename columns for consistency
        buyer_volume.rename(columns={'buyer': 'broker_id', 'quantity': 'buy_volume'}, inplace=True)
        seller_volume.rename(columns={'seller': 'broker_id', 'quantity': 'sell_volume'}, inplace=True)

        # Merge buyer and seller volumes based on 'broker_id' and 'date'
        total_volume = pd.merge(buyer_volume, seller_volume, on=['broker_id', 'date'], how='outer').fillna(0)
        total_volume['total_volume'] = total_volume['buy_volume'] + total_volume['sell_volume']

        # Keep only required columns
        total_volume = total_volume[['broker_id', 'date', 'total_volume']]

        # Get the latest date in the dataset
        latest_date = total_volume['date'].max()

        # Define time periods
        time_periods = {
            'latest_date': latest_date,
            'latest_week': latest_date - timedelta(days=6),  # Last 7 days
            'latest_month': latest_date - timedelta(days=30),  # Last 30 days
            'latest_6_months': latest_date - timedelta(days=180),  # Last 180 days
        }

        # Save data for each time period
        for period_name, period_start_date in time_periods.items():
            # Filter data for the current time period
            period_data = total_volume[total_volume['date'] >= period_start_date]

            # Aggregate total volume by broker for the time period
            period_volume = period_data.groupby('broker_id')['total_volume'].sum().reset_index()
            period_volume['time_period'] = period_name

            # Save to Database
            broker_volume_instances = [
                BrokerTracker(
                    broker_id=row['broker_id'],
                    date=latest_date,  # Use the latest date as the reference
                    total_volume=row['total_volume'],
                    time_period=row['time_period']
                ) for _, row in period_volume.iterrows()
            ]

            # Use bulk_create to save all instances at once
            BrokerTracker.objects.bulk_create(broker_volume_instances)

            self.stdout.write(self.style.SUCCESS(f'Successfully stored {len(broker_volume_instances)} records for {period_name} in the database.'))

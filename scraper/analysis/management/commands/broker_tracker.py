from django.core.management.base import BaseCommand
import dask.dataframe as dd
from analysis.models import BrokerVolume
from datetime import datetime, timedelta

class Command(BaseCommand):
    help = 'Process and store broker volume data from a fixed CSV file using Dask'

    def handle(self, *args, **kwargs):
        # Define the fixed CSV path here
        csv_path = 'C:/Users/Arjun/Desktop/project2/scraper/analysis/floorsheet_floorsheetdata.csv'  # Update with your CSV file path
        
        # Clear existing data in the BrokerVolume table
        self.clear_existing_data()
        
        # Load, process, and store data
        df = self.load_floorsheet_data(csv_path)
        self.process_and_store_data(df)
        self.stdout.write(self.style.SUCCESS(f'Successfully processed and stored data from {csv_path}'))

    def clear_existing_data(self):
        """Clear all existing data in the BrokerVolume table."""
        BrokerVolume.objects.all().delete()
        self.stdout.write(self.style.SUCCESS('Successfully cleared existing data in the BrokerVolume table.'))

    def load_floorsheet_data(self, csv_path):
        """Load the CSV data into a Dask DataFrame with explicit dtypes."""
        # Specify dtypes for problematic columns
        dtypes = {
            'buyer': 'object',  # Read as string
            'seller': 'object',  # Read as string
            'quantity': 'object',  # Read as string first
            'rate': 'object',      # Read as string first
        }
        return dd.read_csv(csv_path, dtype=dtypes)

    def process_and_store_data(self, df):
        """Process the CSV data and store it in the database."""
        df = df.dropna()
        df.columns = df.columns.str.strip()

        # Ensure required columns exist
        if not all(col in df.columns for col in ["buyer", "seller", "quantity", "date"]):
            raise KeyError("Missing required column(s) in data.")

        # Preprocess columns to remove commas and convert to numeric types
        df['quantity'] = df['quantity'].str.replace(',', '').astype(float)
        df['rate'] = df['rate'].str.replace(',', '').astype(float)

        # Convert other columns to appropriate types
        df['buyer'] = df['buyer'].astype(str)
        df['seller'] = df['seller'].astype(str)
        df['date'] = dd.to_datetime(df['date']).dt.date

        # Calculate buyer and seller volumes
        buyer_volume = df.groupby(['buyer', 'date'])['quantity'].sum().reset_index()
        seller_volume = df.groupby(['seller', 'date'])['quantity'].sum().reset_index()

        # Merge buyer and seller volumes
        total_volume = buyer_volume.merge(seller_volume, left_on=['buyer', 'date'], right_on=['seller', 'date'], how='outer').fillna(0)
        total_volume['total_volume'] = total_volume['quantity_x'] + total_volume['quantity_y']
        total_volume = total_volume[['buyer', 'date', 'total_volume']]
        total_volume.columns = ['broker_id', 'date', 'total_volume']

        # Compute the Dask DataFrame to get a Pandas DataFrame
        total_volume = total_volume.compute()

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
                BrokerVolume(
                    broker_id=row['broker_id'],
                    date=latest_date,  # Use the latest date as the reference
                    total_volume=row['total_volume'],
                    time_period=row['time_period']
                ) for _, row in period_volume.iterrows()
            ]

            # Use bulk_create to save all instances at once
            BrokerVolume.objects.bulk_create(broker_volume_instances)

            self.stdout.write(self.style.SUCCESS(f'Successfully stored {len(broker_volume_instances)} records for {period_name} in the database.'))
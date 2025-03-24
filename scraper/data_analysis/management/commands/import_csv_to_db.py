import pandas as pd
from datetime import datetime
from django.core.management.base import BaseCommand
from analysis.models import FloorsheetData  # Replace 'analysis' with your app name

class Command(BaseCommand):
    help = 'Import data from a CSV file into the database'

    def add_arguments(self, parser):
        parser.add_argument('csv_path', type=str, help='The path to the CSV file')

    def handle(self, *args, **kwargs):
        csv_path = kwargs['csv_path']

        # Read the CSV file
        df = pd.read_csv(csv_path)

        # Print the first few rows for debugging
        self.stdout.write("Columns in the CSV file: " + str(df.columns.tolist()))
        self.stdout.write("First few rows of the CSV file:")
        self.stdout.write(str(df.head()))

        # Strip leading/trailing spaces from column names
        df.columns = df.columns.str.strip()

        # Check if required columns exist
        required_columns = ['symbol', 'buyer', 'seller', 'quantity', 'rate', 'amount', 'date']
        for column in required_columns:
            if column not in df.columns:
                raise ValueError(f"Column '{column}' not found in CSV file. Available columns: {df.columns.tolist()}")

        # Drop the 'transaction_no' column
        if 'transaction_no' in df.columns:
            df = df.drop(columns=['transaction_no'])
            self.stdout.write("Dropped 'transaction_no' column.")
        else:
            self.stdout.write("Column 'transaction_no' not found in the CSV file.")

        # Convert the date column to datetime
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

        # Clean numeric columns (remove commas and convert to float)
        numeric_columns = ['quantity', 'rate', 'amount']
        for column in numeric_columns:
            df[column] = df[column].str.replace(',', '').astype(float)

        # Save data to the database
        for _, row in df.iterrows():
            # Create a new record
            FloorsheetData.objects.create(
                symbol=row['symbol'],
                buyer=row['buyer'],
                seller=row['seller'],
                quantity=row['quantity'],
                rate=row['rate'],
                amount=row['amount'],
                date=row['date'],
            )
            self.stdout.write(f"Saved record: {row['symbol']}, {row['buyer']}, {row['seller']}, {row['quantity']}, {row['rate']}, {row['amount']}, {row['date']}")

        self.stdout.write(self.style.SUCCESS('CSV data imported to the database successfully!'))
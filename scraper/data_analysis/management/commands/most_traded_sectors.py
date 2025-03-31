from django.core.management.base import BaseCommand
from data_analysis.models import FloorsheetData, SectorData, MostTradedSector
import pandas as pd

class Command(BaseCommand):
    help = "Calculates and stores the most traded sectors"

    def handle(self, *args, **kwargs):
        self.stdout.write("Starting analysis for most traded sectors...")

        # Fetch transaction data
        queryset = FloorsheetData.objects.values('symbol', 'quantity')
        df = pd.DataFrame(list(queryset))

        if df.empty:
            self.stdout.write(self.style.WARNING("No transaction data available."))
            return

        # Fetch sector data
        sector_queryset = SectorData.objects.values('sector_description', 'symbol')
        sector_data = self.process_sector_data(sector_queryset)

        if sector_data.empty:
            self.stdout.write(self.style.WARNING("No sector data available."))
            return

        # Compute most traded sectors
        most_traded_sectors = self.calculate_most_traded_sectors(df, sector_data)

        if most_traded_sectors.empty:
            self.stdout.write(self.style.WARNING("No traded sectors found."))
            return

        # Save to DB
        self.save_to_db(most_traded_sectors)

        self.stdout.write(self.style.SUCCESS(f"Analysis completed. {len(most_traded_sectors)} records saved."))

    def process_sector_data(self, sector_queryset):
        """ Convert sector symbols (comma-separated) into DataFrame """
        sector_list = []
        for sector in sector_queryset:
            symbols = sector['symbol'].split(', ')
            for symbol in symbols:
                sector_list.append({'sector_description': sector['sector_description'], 'symbol': symbol})

        return pd.DataFrame(sector_list)

    def calculate_most_traded_sectors(self, df, sector_data):
        """ Merge transactions with sector data and rank sectors by traded quantity """
        df_merged = df.merge(sector_data, on='symbol', how='inner')

        sector_volume = df_merged.groupby('sector_description', as_index=False)['quantity'].sum()
        sector_volume = sector_volume.sort_values(by='quantity', ascending=False).reset_index(drop=True)
        sector_volume.insert(0, 'rank', range(1, len(sector_volume) + 1))

        return sector_volume

    def save_to_db(self, most_traded_sectors):
        """ Store results in MostTradedSector model """
        MostTradedSector.objects.all().delete()  # Clear old data

        records = [
            MostTradedSector(
                rank=row['rank'],
                sector_description=row['sector_description'],
                quantity=row['quantity']
            )
            for _, row in most_traded_sectors.iterrows()
        ]
        MostTradedSector.objects.bulk_create(records)
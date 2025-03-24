import pandas as pd
import dask.dataframe as dd
from datetime import timedelta
from django.db.models import Sum
from data_analysis.models import FloorsheetData, StockwiseBroker

def get_stock_names():
    """
    Fetch unique stock symbols from the database.
    """
    stock_names = FloorsheetData.objects.values_list('symbol', flat=True).distinct()
    return stock_names

def process_stock_data_with_dask(stock_name, time_frame):
    """
    Process stock data for a given stock and time frame using Dask.
    Returns a DataFrame with broker volumes, percent volume, and date range.
    """
    # Fetch data for the selected stock
    stock_data = FloorsheetData.objects.filter(symbol=stock_name)

    if not stock_data.exists():
        print(f"No data found for stock: {stock_name}")  # Debugging
        return None, None

    # Convert Django QuerySet to Dask DataFrame
    stock_data_df = pd.DataFrame(list(stock_data.values()))
    dask_df = dd.from_pandas(stock_data_df, npartitions=10)

    # Convert the 'date' column to datetime and remove time component
    dask_df['date'] = dd.to_datetime(dask_df['date']).dt.date

    # Get the latest date in the data
    latest_date = dask_df['date'].max().compute()
    print(f"Latest date in data: {latest_date}")  # Debugging

    # Define time frames
    if time_frame == "daily":
        start_date = latest_date
    elif time_frame == "latest_week":
        start_date = latest_date - timedelta(days=4)
    elif time_frame == "latest_month":
        start_date = latest_date - timedelta(days=29)
    elif time_frame == "latest_3_months":
        start_date = latest_date - timedelta(days=89)
    elif time_frame == "latest_6_months":
        start_date = latest_date - timedelta(days=179)
    else:
        raise ValueError("Invalid time frame selected.")

    print(f"Filtering data from {start_date} to {latest_date}")  # Debugging

    # Filter data for the selected time frame
    filtered_df = dask_df[(dask_df['date'] >= start_date) & (dask_df['date'] <= latest_date)]

    # Calculate total volume for each broker as a buyer
    buyer_volume = filtered_df.groupby('buyer')['quantity'].sum().compute().reset_index()
    buyer_volume_df = buyer_volume.rename(columns={"buyer": "broker_id", "quantity": "buy_volume"})

    # Calculate total volume for each broker as a seller
    seller_volume = filtered_df.groupby('seller')['quantity'].sum().compute().reset_index()
    seller_volume_df = seller_volume.rename(columns={"seller": "broker_id", "quantity": "sell_volume"})

    # Merge buyer and seller volumes
    total_volume = pd.merge(buyer_volume_df, seller_volume_df, on="broker_id", how="outer").fillna(0)

    # Calculate total volume for each broker (buy_volume + sell_volume)
    total_volume["total_volume"] = total_volume["buy_volume"] + total_volume["sell_volume"]

    # Calculate grand total volume of all brokers over the selected time frame
    grand_total_volume = total_volume["total_volume"].sum()

    # Calculate percentage volume for each broker
    total_volume["percent_volume"] = (total_volume["total_volume"] / grand_total_volume) * 100

    # Sort by total_volume in descending order
    total_volume = total_volume.sort_values(by="total_volume", ascending=False)

    # Add the date range for the selected time frame
    if time_frame == "daily":
        total_volume["date_range"] = latest_date.strftime("%b %d, %Y")
    else:
        total_volume["date_range"] = f"{start_date.strftime('%b %d, %Y')} to {latest_date.strftime('%b %d, %Y')}"

    # Round the percentage volume to 2 decimal places
    total_volume["percent_volume"] = total_volume["percent_volume"].round(2)

    print(f"Final processed data: {total_volume}")  # Debugging
    print(f"Grand total volume: {grand_total_volume}")  # Debugging

    return total_volume[["broker_id", "total_volume", "percent_volume", "date_range"]], grand_total_volume

def save_broker_data(stock_name, time_frame):
    """
    Save processed broker data to the database.
    """
    # Process the stock data for the selected time frame using Dask
    result_df, grand_total_volume = process_stock_data_with_dask(stock_name, time_frame)

    if result_df is not None:
        print(f"Data processed successfully. Records found: {len(result_df)}")  # Debugging

        # Delete existing data for the selected stock and time frame
        StockwiseBroker.objects.filter(stock_name=stock_name, time_frame=time_frame).delete()
        print(f"Deleted existing data for stock: {stock_name} and time frame: {time_frame}")  # Debugging

        # Save new data for the selected stock and time frame
        for _, row in result_df.iterrows():
            StockwiseBroker.objects.create(
                stock_name=stock_name,
                broker_id=row['broker_id'],
                volume=row['total_volume'],
                percent_volume=row['percent_volume'],
                date_range=row['date_range'],  # Save the date range
                time_frame=time_frame  # Save the time frame
            )
        print(f"Saved {len(result_df)} records to the database.")  # Debugging
    else:
        print("No data found for the selected stock and time frame.")  # Debugging

def process_and_save_all_stocks(time_frame):
    """
    Process and save data for all stocks for the given time frame.
    """
    stock_names = get_stock_names()
    for stock_name in stock_names:
        print(f"Processing data for stock: {stock_name} and time frame: {time_frame}")  # Debugging
        save_broker_data(stock_name, time_frame)
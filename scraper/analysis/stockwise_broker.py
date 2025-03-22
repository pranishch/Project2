# analysis/stockwise_broker.py
import dask.dataframe as dd
import pandas as pd
from datetime import timedelta


def load_floorsheet_data(csv_path):
    # Specify dtypes for problematic columns
    dtypes = {
        'quantity': 'object',  # Read as string initially
        'rate': 'object',      # Read as string initially
        'buyer': 'str',
        'seller': 'str'
    }
    
    # Load the CSV file with specified dtypes
    df = dd.read_csv(csv_path, dtype=dtypes)
    
    # Clean the data: Remove commas and convert to numeric types
    df['quantity'] = df['quantity'].str.replace(',', '').astype('int64')
    df['rate'] = df['rate'].str.replace(',', '').astype('float64')
    
    return df

def get_stock_names(df):
    possible_columns = ["symbol", "Symbol", "SYMBOL"]  # Checking different cases
    stock_col = next((col for col in possible_columns if col in df.columns), None)

    if stock_col:
        return df[stock_col].dropna().unique().compute(), stock_col
    else:
        raise KeyError("Stock-related column (symbol) not found in the CSV file.")

def process_stock_data(df, stock_name, stock_col):
    df = df.dropna()
    df['date'] = dd.to_datetime(df['date']).dt.date  # Convert to date only (no time)
    
    # Get the latest date and calculate the start date for the last 5 days
    latest_date = df['date'].max().compute()
    start_date = latest_date - timedelta(days=5)
    
    # Filter data for the last 5 days and the selected stock
    stock_df = df[(df[stock_col] == stock_name) & (df['date'] >= start_date)].compute()

    if stock_df.empty:
        return None

    # Calculate buyer and seller volumes
    buyer_volume = stock_df.groupby(['date', 'buyer'])['quantity'].sum().reset_index()
    seller_volume = stock_df.groupby(['date', 'seller'])['quantity'].sum().reset_index()

    buyer_volume = buyer_volume.rename(columns={"buyer": "broker_id", "quantity": "buy_volume"})
    seller_volume = seller_volume.rename(columns={"seller": "broker_id", "quantity": "sell_volume"})

    # Merge buyer and seller volumes
    total_volume = pd.merge(buyer_volume, seller_volume, on=["date", "broker_id"], how="outer").fillna(0)
    total_volume["volume"] = total_volume["buy_volume"] + total_volume["sell_volume"]
    total_volume = total_volume.drop(columns=["buy_volume", "sell_volume"])

    # Calculate total market volume per day
    total_traded_per_day = stock_df.groupby('date')["quantity"].sum().reset_index()
    total_traded_per_day = total_traded_per_day.rename(columns={"quantity": "total_market_volume"})

    # Merge total volume with total market volume
    total_volume = pd.merge(total_volume, total_traded_per_day, on="date", how="left")
    total_volume["percent_volume"] = (total_volume["volume"] / total_volume["total_market_volume"]) * 100

    # Filter for the latest date
    latest_date_data = total_volume[total_volume['date'] == latest_date]
    latest_date_data = latest_date_data[["broker_id", "volume", "percent_volume", "date"]]
    latest_date_data["volume"] = latest_date_data["volume"].astype(int)
    latest_date_data["percent_volume"] = latest_date_data["percent_volume"].round(2)

    return latest_date_data
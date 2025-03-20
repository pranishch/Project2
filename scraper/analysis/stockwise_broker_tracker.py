import pandas as pd

# Load CSV file
def load_floorsheet_data(csv_path):
    df = pd.read_csv(csv_path, dtype={'buyer': str, 'seller': str}, low_memory=False)
    print("\nColumns in CSV file:", df.columns.tolist())  # Debugging step
    return df

# Get available stock symbols from the CSV
def get_stock_names(df):
    possible_columns = ["symbol", "Symbol", "SYMBOL"]  # Checking different cases
    stock_col = next((col for col in possible_columns if col in df.columns), None)

    if stock_col:
        return df[stock_col].dropna().unique(), stock_col
    else:
        raise KeyError("Stock-related column (symbol) not found in the CSV file.")

# Process stock-wise broker volume for last 7 days
def stock_wise_big_broker_tracker(df, stock_name, stock_col):
    df = df.dropna()
    df.columns = df.columns.str.strip()

    required_cols = {stock_col, "buyer", "seller", "quantity", "date"}
    if not required_cols.issubset(df.columns):
        raise KeyError(f"Missing required columns: {required_cols - set(df.columns)}")

    df["quantity"] = pd.to_numeric(df["quantity"], errors='coerce')
    df["date"] = pd.to_datetime(df["date"], errors='coerce')
    df = df.dropna()

    # Remove time component from the date column
    df["date"] = df["date"].dt.date  # Keep only the date part (YYYY-MM-DD)

    # Filter for the selected stock in the last 7 days
    last_week = df["date"].max() - pd.Timedelta(days=7)
    stock_df = df[(df[stock_col] == stock_name) & (df["date"] >= last_week)]

    if stock_df.empty:
        print(f"No data available for symbol: {stock_name} in the last 7 days.")
        return None

    # Compute broker-wise volume (buy + sell) per day
    buyer_volume = stock_df.groupby(['date', 'buyer'])['quantity'].sum().reset_index()
    seller_volume = stock_df.groupby(['date', 'seller'])['quantity'].sum().reset_index()

    # Rename columns
    buyer_volume.rename(columns={"buyer": "Broker ID", "quantity": "Buy Volume"}, inplace=True)
    seller_volume.rename(columns={"seller": "Broker ID", "quantity": "Sell Volume"}, inplace=True)

    # Merge buyer and seller data
    total_volume = pd.merge(buyer_volume, seller_volume, on=["date", "Broker ID"], how="outer").fillna(0)
    total_volume["Volume"] = total_volume["Buy Volume"] + total_volume["Sell Volume"]
    total_volume.drop(columns=["Buy Volume", "Sell Volume"], inplace=True)

    # Compute % of total traded volume for each day
    total_traded_per_day = stock_df.groupby('date')["quantity"].sum().reset_index()
    total_traded_per_day.rename(columns={"quantity": "Total Market Volume"}, inplace=True)

    # Merge with total_volume
    total_volume = pd.merge(total_volume, total_traded_per_day, on="date", how="left")
    total_volume["% Volume"] = (total_volume["Volume"] / total_volume["Total Market Volume"]) * 100

    # Reorder columns: Broker ID, Volume, % Volume, Date
    total_volume = total_volume[["Broker ID", "Volume", "% Volume", "date"]]

    # Format numbers (remove commas, add spacing)
    total_volume["Volume"] = total_volume["Volume"].astype(int)
    total_volume["% Volume"] = total_volume["% Volume"].round(2)

    return total_volume

# Save the CSV file with consistent spacing and stock name
def save_pretty_csv(df, output_path, stock_name):
    # Define column widths (adjust as needed)
    col_widths = {
        "Broker ID": 15,
        "Volume": 10,
        "% Volume": 10,
        "date": 15
    }

    # Create a formatted header string
    header = "".join([col.ljust(col_widths[col]) for col in df.columns])
    header = header.rstrip()  # Remove trailing spaces

    # Create formatted data rows
    formatted_rows = []
    for _, row in df.iterrows():
        row_str = "".join([str(row[col]).ljust(col_widths[col]) for col in df.columns])
        formatted_rows.append(row_str.rstrip())  # Remove trailing spaces

    # Combine stock name, header, and rows into a single string
    formatted_output = f"Stock: {stock_name}\n\n"  # Add stock name at the top
    formatted_output += header + "\n" + "\n".join(formatted_rows)

    # Save the formatted string to a file
    with open(output_path, "w") as file:
        file.write(formatted_output)

    print(f"\nStock-wise broker tracker saved to {output_path}")

# Main function
def main():
    csv_path = r"C:\Users\Arjun\Desktop\project2\scraper\analysis\floorsheet_floorsheetdata.csv"
    df = load_floorsheet_data(csv_path)

    # Detect correct stock column (symbol)
    stock_names, stock_col = get_stock_names(df)

    print("\nAvailable Symbols:")
    for stock in stock_names:
        print(stock)

    stock_name = input("\nEnter the symbol from the above list: ").strip()

    result_df = stock_wise_big_broker_tracker(df, stock_name, stock_col)

    if result_df is not None:
        output_path = r"C:\Users\Arjun\Desktop\project2\scraper\analysis\stock_wise_broker_tracker.txt"
        save_pretty_csv(result_df, output_path, stock_name)  # Pass stock_name to the function
        print(f"\nStock-wise broker tracker saved to {output_path}")

# Run script
if __name__ == "__main__":
    main()
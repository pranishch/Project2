import pandas as pd

# Load CSV file
def load_floorsheet_data(csv_path):
    return pd.read_csv(csv_path)

# Process and calculate broker volume
def process_data(df):
    df = df.dropna()
    df.columns = df.columns.str.strip()
    
    if not all(col in df.columns for col in ["buyer", "seller", "quantity"]):
        raise KeyError("Missing required column(s) in data.")
    
    df["quantity"] = pd.to_numeric(df["quantity"], errors='coerce').dropna()
    df['buyer'] = df['buyer'].astype(str)
    df['seller'] = df['seller'].astype(str)
    
    # Compute total volume per broker
    buyer_volume = df.groupby('buyer')['quantity'].sum()
    seller_volume = df.groupby('seller')['quantity'].sum()
    
    # Combine buy and sell volumes into total volume
    total_volume = buyer_volume.add(seller_volume, fill_value=0).reset_index()
    total_volume.columns = ['Broker ID', 'Total Volume']
    
    return total_volume

# Main function
def main():
    csv_path = r"C:\Users\Arjun\Desktop\project2\scraper\analysis\floorsheet_floorsheetdata.csv"
    df = load_floorsheet_data(csv_path)
    broker_volume_df = process_data(df)
    
    output_path = r"C:\Users\Arjun\Desktop\project2\scraper\analysis\broker_volume_data.csv"
    broker_volume_df.to_csv(output_path, index=False)
    print(f"Broker Volume data saved to {output_path}")

# Run script
if __name__ == "__main__":
    main()

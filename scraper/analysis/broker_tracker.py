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
    
    # Unique broker list and ID mapping
    unique_brokers = sorted(set(df['buyer']).union(set(df['seller'])))
    broker_id_map = {broker: idx + 1 for idx, broker in enumerate(unique_brokers)}
    
    df['Buyer Broker ID'] = df['buyer'].map(broker_id_map)
    df['Seller Broker ID'] = df['seller'].map(broker_id_map)
    
    # Compute volume per broker
    buyer_volume = df.groupby('Buyer Broker ID')['quantity'].sum().reset_index()
    seller_volume = df.groupby('Seller Broker ID')['quantity'].sum().reset_index()
    
    # Merge buyer & seller volumes
    broker_volume = pd.concat([buyer_volume, seller_volume])
    broker_volume = broker_volume.groupby('Buyer Broker ID')['quantity'].sum().reset_index()
    broker_volume.columns = ['Broker ID', 'Volume']
    
    return broker_volume

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

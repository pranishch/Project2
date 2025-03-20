import pandas as pd

# Load CSV file
def load_floorsheet_data(csv_path):
    """
    Load the CSV file into a pandas DataFrame.
    """
    df = pd.read_csv(csv_path)
    return df

# Data Cleaning and Processing
def process_data(df):
    """
    Clean and process the data, calculate broker IDs, and compute total volume for each broker.
    """
    # Drop rows with missing data
    df = df.dropna()

    # Remove extra spaces from column names
    df.columns = df.columns.str.strip()

    # Ensure required columns exist
    required_columns = ["buyer", "seller", "quantity"]
    for col in required_columns:
        if col not in df.columns:
            raise KeyError(f"Missing required column: {col}")

    # Convert quantity to integer (if it's not already)
    df["quantity"] = pd.to_numeric(df["quantity"], errors='coerce')  # Converts to numeric, coercing errors to NaN
    df = df.dropna(subset=["quantity"])  # Drop rows where quantity could not be converted

    # Calculate Volume from Quantity (total number of items in transaction)
    df["Volume"] = df["quantity"]

    # Convert buyer and seller columns to string to ensure consistent types
    df['buyer'] = df['buyer'].astype(str)
    df['seller'] = df['seller'].astype(str)

    # Step 1: Identify unique buyers and sellers
    unique_buyers = set(df['buyer'].unique())
    unique_sellers = set(df['seller'].unique())

    # Debug: Print number of unique buyers and sellers
    print(f"Number of unique buyers: {len(unique_buyers)}")
    print(f"Number of unique sellers: {len(unique_sellers)}")

    # Step 2: Combine unique buyers and sellers into a single set (union)
    unique_brokers = unique_buyers.union(unique_sellers)

    # Debug: Print number of unique brokers after combining
    print(f"Total number of unique brokers after combining: {len(unique_brokers)}")

    # Step 3: Assign a unique Broker ID to each unique broker
    # Convert all broker names to strings before sorting
    unique_brokers_str = [str(broker) for broker in unique_brokers]
    sorted_brokers = sorted(unique_brokers_str)
    broker_id_map = {broker: idx + 1 for idx, broker in enumerate(sorted_brokers)}

    # Debug: Print a sample of the broker ID mapping
    print("Sample Broker ID Mapping:")
    for broker, broker_id in list(broker_id_map.items())[:5]:  # Print first 5 entries
        print(f"{broker}: {broker_id}")

    # Step 4: Map Broker IDs to buyers and sellers
    df['Buyer Broker ID'] = df['buyer'].map(broker_id_map)
    df['Seller Broker ID'] = df['seller'].map(broker_id_map)

    # Step 5: Calculate volume for each broker (as either buyer or seller)
    buyer_volume = df.groupby('Buyer Broker ID')['Volume'].sum().reset_index()
    seller_volume = df.groupby('Seller Broker ID')['Volume'].sum().reset_index()

    # Step 6: Combine buyer and seller volumes into a single DataFrame
    buyer_volume.columns = ['Broker ID', 'Volume']
    seller_volume.columns = ['Broker ID', 'Volume']
    
    # Concatenate and group by Broker ID to get total volume
    broker_volume = pd.concat([buyer_volume, seller_volume])
    broker_volume = broker_volume.groupby('Broker ID')['Volume'].sum().reset_index()

    # Step 7: Sort by total volume in descending order
    broker_volume = broker_volume.sort_values(by='Volume', ascending=False).reset_index(drop=True)

    return broker_volume, broker_id_map, df

# Main execution function
def main():
    # Path to the input CSV file
    csv_path = r"C:\Users\Arjun\Desktop\project2\scraper\scrap\floorsheet_floorsheetdata.csv"

    # Load the data
    df = load_floorsheet_data(csv_path)

    # Process the data and get broker volume data, broker ID mapping, and enhanced DataFrame
    broker_volume_df, broker_id_map, enhanced_df = process_data(df)

    # Save the broker volume data to a new CSV file (without Broker Name)
    output_volume_path = r"C:\Users\Arjun\Desktop\project2\scraper\scrap\broker_volume_data.csv"
    broker_volume_df.to_csv(output_volume_path, index=False)
    print(f"Broker Volume data saved to {output_volume_path}")

    # Save the enhanced DataFrame with broker IDs
    output_enhanced_path = r"C:\Users\Arjun\Desktop\project2\scraper\scrap\enhanced_floorsheet_data.csv"
    enhanced_df.to_csv(output_enhanced_path, index=False)
    print(f"Enhanced floorsheet data with broker IDs saved to {output_enhanced_path}")

    # Example: Retrieve broker ID for a specific broker
    if broker_id_map:
        sample_broker_name = list(broker_id_map.keys())[0]  # Get the first broker name in the map
        broker_id = broker_id_map.get(sample_broker_name)
        print(f"Sample: Broker ID for {sample_broker_name} is: {broker_id}")

# Run the main function
if __name__ == "__main__":
    main()
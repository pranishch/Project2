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

# Function to save DataFrame with consistent spacing
def save_with_spacing(df, output_path, spacing=8):
    # Determine the maximum width for each column
    col_widths = [max(len(str(col)), df[col].astype(str).str.len().max()) for col in df.columns]
    
    # Create a format string for consistent spacing
    format_str = ' '.join([f'{{:<{width + spacing}}}' for width in col_widths])
    
    # Write headers with consistent spacing
    headers = format_str.format(*df.columns)
    
    # Write data rows with consistent spacing
    data_rows = []
    for _, row in df.iterrows():
        data_row = format_str.format(*row)
        data_rows.append(data_row)
    
    # Combine headers and data rows
    content = headers + '\n' + '\n'.join(data_rows)
    
    # Write to file
    with open(output_path, 'w') as f:
        f.write(content)

# Main function
def main():
    csv_path = r"C:\Users\Arjun\Desktop\project2\scraper\analysis\floorsheet_floorsheetdata.csv"
    df = load_floorsheet_data(csv_path)
    broker_volume_df = process_data(df)
    
    output_path = r"C:\Users\Arjun\Desktop\project2\scraper\analysis\broker_volume_data.csv"
    save_with_spacing(broker_volume_df, output_path, spacing=8)  # Adjust spacing as needed
    print(f"Broker Volume data saved to {output_path}")

# Run script
if __name__ == "__main__":
    main()
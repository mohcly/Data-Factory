import pandas as pd
import json
from datetime import datetime
import os

def convert_csv_to_json():
    # Read the CSV file
    df = pd.read_csv('data/btcusdt/btcusdt_1m.csv')
    
    # Convert timestamp to datetime (assuming it's already in datetime string format)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')
    
    # Split data into chunks of 60 minutes (prediction_length)
    chunk_size = 60
    n_chunks = len(df) // chunk_size
    
    # Create training samples
    train_samples = []
    for i in range(n_chunks):
        start_idx = i * chunk_size
        end_idx = start_idx + chunk_size
        chunk_data = df.iloc[start_idx:end_idx]
        
        if len(chunk_data) == chunk_size:  # Ensure we have complete chunks
            sample = {
                "start": chunk_data['timestamp'].iloc[0].strftime("%Y-%m-%d %H:%M:%S"),
                "target": chunk_data['close'].tolist(),
                "item_id": f"closing_price_{i}"
            }
            train_samples.append(sample)
    
    # Create the JSON structure
    json_data = {
        "train": train_samples,
        "metadata": {
            "freq": "T",  # T represents minutes in pandas frequency
            "prediction_length": 60
        }
    }
    
    # Save to JSON file
    output_path = 'data/datasets/btcusdt_1m_closing.json'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(json_data, f, indent=4)
    
    print(f"Conversion completed. JSON file created at: {output_path}")
    print(f"Dataset contains {len(train_samples)} training samples")
    print(f"Each sample contains {chunk_size} price points")
    print(f"First sample starts at: {train_samples[0]['start']}")

if __name__ == "__main__":
    convert_csv_to_json() 
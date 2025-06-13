import pandas as pd
import numpy as np
import argparse
import sys
import os
from datetime import datetime
import pytz

def extract_temporal_features(df):
    """
    Extract temporal features from the timestamp
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing 'tmpz' column
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame with added temporal features
    """
    print("Extracting temporal features...")
    
    # Convert 'tmpz' to datetime objects
    df['timestamp'] = pd.to_datetime(df['tmpz'], format='%d/%b/%Y:%H:%M:%S %z', errors='coerce')
    
    # Extract additional temporal features
    df['hour'] = df['timestamp'].dt.hour
    df['date'] = df['timestamp'].dt.date
    df['day_of_week'] = df['timestamp'].dt.day_name()
    
    # Extract timezone information
    df['timezone_offset'] = df['tmpz'].str.extract(r'(\+\d{4}|\-\d{4})$')
    
    # Map timezone offsets to approximate geographic regions
    timezone_mapping = {
        '+0000': 'UTC/GMT',
        '-0800': 'North America/Pacific',
        '-0700': 'North America/Mountain',
        '-0600': 'North America/Central',
        '-0500': 'North America/Eastern',
        '+0100': 'Europe/Western',
        '+0200': 'Europe/Eastern',
        '+0300': 'Middle East',
        '+0530': 'South Asia',
        '+0800': 'East Asia',
        '+0900': 'Japan/Korea',
        '+1000': 'Australia/Eastern',
        '+1200': 'Pacific/New Zealand'
    }
    
    df['gps_zone'] = df['timezone_offset'].map(timezone_mapping).fillna('Unknown')
    
    return df

def classify_ip_type(ip):
    """
    Simple classifier for IP types
    
    Parameters:
    -----------
    ip : str
        IP address
        
    Returns:
    --------
    str
        Classification of the IP
    """
    # This is a simplified version - in production you would use a more robust approach
    if ip.startswith('192.168.') or ip.startswith('10.') or ip.startswith('172.'):
        return 'Private'
    elif ip == '127.0.0.1':
        return 'Localhost'
    else:
        return 'Public'

def add_request_analysis(df):
    """
    Add analysis of request data
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame with request data
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame with additional request analysis
    """
    print("Analyzing requests...")
    
    # Count requests by IP
    request_counts = df.groupby('ip').size().reset_index(name='request_count')
    df = df.merge(request_counts, on='ip', how='left')
    
    # Classify IPs
    df['ip_type'] = df['ip'].apply(classify_ip_type)
    
    # Extract file extension from path
    df['file_extension'] = df['path'].str.extract(r'\.([a-zA-Z0-9]+)(?:\?|$)')
    
    # Categorize status codes
    def categorize_status(code):
        code = int(code)
        if 200 <= code < 300:
            return 'Success'
        elif 300 <= code < 400:
            return 'Redirect'
        elif 400 <= code < 500:
            return 'Client Error'
        elif 500 <= code < 600:
            return 'Server Error'
        else:
            return 'Unknown'
    
    df['status_category'] = df['status_code'].apply(categorize_status)
    
    return df

def process_logs_data(df, batch_size=100):
    """
    Process logs data with all transformations
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Input DataFrame with log data
    batch_size : int
        Batch size for processing (simulation)
        
    Returns:
    --------
    pandas.DataFrame
        Processed DataFrame
    """
    print(f"Processing logs data in batches of {batch_size}...")
    
    # Process in batches to simulate handling large datasets
    total_batches = len(df) // batch_size + (1 if len(df) % batch_size > 0 else 0)
    processed_dfs = []
    
    for i in range(total_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, len(df))
        batch = df.iloc[start_idx:end_idx].copy()
        
        print(f"Processing batch {i+1}/{total_batches} (records {start_idx}-{end_idx})")
        
        # Apply transformations
        batch = extract_temporal_features(batch)
        batch = add_request_analysis(batch)
        
        processed_dfs.append(batch)
    
    # Combine all processed batches
    result = pd.concat(processed_dfs, ignore_index=True)
    
    # Final cleaning
    # Drop any rows with NaN in crucial columns
    result = result.dropna(subset=['ip', 'timestamp', 'path'])
    
    print(f"Finished processing. Final dataset has {len(result)} rows and {len(result.columns)} columns.")
    return result

def main():
    parser = argparse.ArgumentParser(description='Transform Apache logs data')
    parser.add_argument('--input-file', required=True, help='Path to the input CSV file')
    parser.add_argument('--output-file', required=True, help='Path to save the output CSV file')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for processing')
    
    args = parser.parse_args()
    
    # Validate input file exists
    if not os.path.exists(args.input_file):
        print(f"Error: Input file not found: {args.input_file}")
        sys.exit(1)
    
    try:
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(args.output_file), exist_ok=True)
        
        # Read the CSV data
        print(f"Reading data from {args.input_file}...")
        df = pd.read_csv(args.input_file)
        print(f"Read {len(df)} records with {len(df.columns)} columns.")
        
        # Process the data
        processed_df = process_logs_data(df, args.batch_size)
        
        # Save to output file
        print(f"Saving transformed data to {args.output_file}...")
        processed_df.to_csv(args.output_file, index=False)
        print(f"Transformation complete. Data saved to {args.output_file}")
        
        # Print summary stats
        print("\nData Summary:")
        print(f"- Total records: {len(processed_df)}")
        print(f"- Unique IPs: {processed_df['ip'].nunique()}")
        print(f"- Date range: {processed_df['date'].min()} to {processed_df['date'].max()}")
        print(f"- Most common browser: {processed_df['navigator'].value_counts().index[0]}")
        
    except Exception as e:
        print(f"Error transforming data: {str(e)}")
        sys.exit(1)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
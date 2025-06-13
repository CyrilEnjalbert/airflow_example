import argparse
import pandas as pd
import re
import os
import sys

def parse_apache_logs(file_path):
    """
    Parse Apache logs dynamically to extract the fields as specified in requirements
    
    Parameters:
    -----------
    file_path : str
        Path to the Apache log file
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame containing parsed log data
    """
    # Define pattern for Apache log format based on the specified fields
    pattern = r'(\S+) \S+ \S+ \[(.*?)\] "(\w+) (.*?) (.*?)" (\d+) (\d+) "(.*?)" "(.*?)"'
    
    # Lists for storing parsed data
    data = {
        'ip': [],
        'tmpz': [],
        'request_type': [],
        'path': [],
        'http_client': [],
        'status_code': [],
        'latency': [],
        'url': [],
        'user_agent': []
    }
    
    print(f"Reading log file: {file_path}")
    
    # Parse the log file
    with open(file_path, 'r') as file:
        line_count = 0
        parsed_count = 0
        
        for line in file:
            line_count += 1
            match = re.match(pattern, line.strip())
            
            if match:
                parsed_count += 1
                data['ip'].append(match.group(1))
                data['tmpz'].append(match.group(2))
                data['request_type'].append(match.group(3))
                data['path'].append(match.group(4))
                data['http_client'].append(match.group(5))
                data['status_code'].append(int(match.group(6)))
                data['latency'].append(int(match.group(7)))
                data['url'].append(match.group(8))
                data['user_agent'].append(match.group(9))
    
    print(f"Read {line_count} lines, successfully parsed {parsed_count} lines")
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Extract navigator safely using regex
    def extract_navigator(user_agent):
        browsers = ['Chrome', 'Firefox', 'Safari', 'Edge', 'Opera', 'MSIE']
        for browser in browsers:
            if browser.lower() in user_agent.lower():
                return browser
        return 'Other'
    
    df['navigator'] = df['user_agent'].apply(extract_navigator)
    
    return df

def main():
    """Process Apache logs and convert to CSV format"""
    parser = argparse.ArgumentParser(description='Parse Apache logs and convert to CSV')
    parser.add_argument('--input-file', required=True, help='Path to the input log file')
    parser.add_argument('--output-file', required=True, help='Path to save the output CSV file')
    
    args = parser.parse_args()
    
    # Validate input file exists
    if not os.path.exists(args.input_file):
        print(f"Error: Input file not found: {args.input_file}")
        sys.exit(1)
    
    try:
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(args.output_file), exist_ok=True)
        
        # Parse the logs into a DataFrame
        print(f"Parsing logs from {args.input_file}...")
        df = parse_apache_logs(args.input_file)
        
        # Save the DataFrame to CSV
        print(f"Saving {len(df)} records to {args.output_file}...")
        df.to_csv(args.output_file, index=False)
        
        print(f"CSV output saved successfully to {args.output_file}")
        print(f"Data preview (first 5 rows):\n{df.head()}")
        
    except Exception as e:
        print(f"Error processing logs: {str(e)}")
        sys.exit(1)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
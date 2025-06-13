## Extraction
# Import the requests module
import requests
import os
import pandas as pd
import json
import time
import numpy as np
import argparse
import sys
from requests.exceptions import RequestException

def extract_from_url_to_csv(url, output_folder):
    """
    Fetches the content from the given URL and saves it to a file.
    
    Parameters:
        url (str): The URL to fetch the content from.
        output_folder (str): The folder where the output file will be saved.
    
    Returns:
        bool: True if successful, False otherwise.
    """
    try:
        # Ensure the output folder exists
        try:
            os.makedirs(output_folder, exist_ok=True)
            print(f"Output directory confirmed: {output_folder}")
        except PermissionError:
            print(f"Permission denied when creating directory: {output_folder}")
            return False
        except Exception as e:
            print(f"Error creating output directory: {str(e)}")
            return False

        # Fetch the content from the URL with timeout and retries
        max_retries = 3
        retry_delay = 2  # seconds
        
        for attempt in range(max_retries):
            try:
                print(f"Attempting to download from {url} (attempt {attempt+1}/{max_retries})")
                response = requests.get(url, timeout=30)
                if response.status_code != 200:
                    if attempt < max_retries - 1:
                        print(f"Attempt {attempt+1} failed with status code {response.status_code}. Retrying in {retry_delay}s...")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        print(f"Failed to download after {max_retries} attempts. Status code: {response.status_code}")
                        return False
                else:
                    break  # Success, break the loop
            except RequestException as e:
                if attempt < max_retries - 1:
                    print(f"Attempt {attempt+1} failed: {str(e)}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    print(f"Failed to download after {max_retries} attempts: {str(e)}")
                    return False

        # Process the successful response
        apache_logs = response.text
        
        if not apache_logs.strip():
            print("Warning: Downloaded content is empty")
    
        # Construct the output file path
        output_file = os.path.join(output_folder, "apache_logs.txt")
        print(f"{output_file} will be created to store the Apache logs.")
        
        # Save the content to a file
        try:
            with open(output_file, "w") as file:
                file.write(apache_logs)
        except IOError as e:
            print(f"IO Error when writing to file {output_file}: {str(e)}")
            return False
        
        # Get file stats
        try:
            file_size = os.path.getsize(output_file) / 1024  # Size in KB
            line_count = len(apache_logs.splitlines())
            
            # Print information about the download
            print(f"File downloaded successfully!")
            print(f"Saved to: {os.path.abspath(output_file)}")
            print(f"File size: {file_size:.2f} KB")
            print(f"Number of lines: {line_count}")
            
            # Display first 5 lines as a sample
            print("\nSample of the first 5 lines:")
            for line in apache_logs.splitlines()[:5]:
                print(line)
                
            return True
        except Exception as e:
            print(f"Error getting file statistics: {str(e)}")
            return False

    except Exception as e:
        print(f"Unexpected error during extraction: {str(e)}")
        return False


def main():
    """Command line interface for the extraction script."""
    parser = argparse.ArgumentParser(description='Extract Apache logs from a URL and save to a file')
    parser.add_argument('--source-url', required=True, 
                        help='URL where the Apache logs are hosted')
    parser.add_argument('--output-folder', required=True,
                        help='Folder where the output file will be saved')
    
    args = parser.parse_args()
    
    success = extract_from_url_to_csv(args.source_url, args.output_folder)
    
    if success:
        print("Extraction completed successfully")
        sys.exit(0)
    else:
        print("Extraction failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
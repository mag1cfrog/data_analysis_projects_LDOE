import os
import json
from datetime import datetime



def reset_log():
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    log_file = f'process_log_{timestamp}.txt'
    return log_file
    

def delete_file(filepath):
    # Check if the file exists
    if os.path.exists(filepath):
        # Delete the file
        os.remove(filepath)
        print(f"The cached file has been deleted.")
    else:
        print(f"File {filepath} does not exist.")


def load_configuration(config_file):
    """Load and return configuration settings from a file."""
    with open(config_file, 'r') as f:
        return json.load(f)
    

def export_results(df, config):
    """Export the processed data to specified formats and locations."""
    df.to_parquet(config['parquet_output_location'])
    df.to_csv(config['csv_output_location'])


def post_processing_cleanup(config):
    """Handle the deletion of temporary files."""
    filepath = os.path.join(config["Directory"], 'cache', 'combined_file.pdf')
    confirm = input(f"Do you want to delete the cached file {filepath}? (y/n) \n")
    if confirm.lower() == 'y':
        delete_file(filepath)
    
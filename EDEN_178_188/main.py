import os
from duckdb_operation import duckdb_data_extract_clean
from data_calculation import aggregated_data_calculation
from report_formatting import formatting_reports
from utils import load_config, time_function

# Config file location
CONFIG_LOCATION = "EDEN/23/178_188/performance_optimization/config.json"

def write_df_to_csv(df, directory, name):
    filename = f'{name}.csv'
    complete_location = os.path.join(directory, filename)
    df.write_csv(complete_location, include_header=False)


def process_and_export_data(student_roster_178_df, student_roster_188_df, config):
    # aggregation by subgroups
    intermediate_list = aggregated_data_calculation(student_roster_178_df, student_roster_188_df, config)
    format_and_export_reports(intermediate_list, config)


def format_and_export_reports(intermediate_list, config):
    # Format the data for export
    (formatted_school_178, 
    formatted_district_178,
    formatted_state_178,
    formatted_school_188, 
    formatted_district_188,
    formatted_state_188) = formatting_reports(intermediate_list, config)
    # Export the data
    write_df_to_csv(formatted_school_178, config['178_output_directory'], config['178_filename']['school'])
    write_df_to_csv(formatted_district_178, config['178_output_directory'], config['178_filename']['district'])
    write_df_to_csv(formatted_state_178, config['178_output_directory'], config['178_filename']['state'])
    write_df_to_csv(formatted_school_188, config['188_output_directory'], config['188_filename']['school'])
    write_df_to_csv(formatted_district_188, config['188_output_directory'], config['188_filename']['district'])
    write_df_to_csv(formatted_state_188, config['188_output_directory'], config['188_filename']['state'])


@time_function
def main():
    # Load the config file
    config = load_config(CONFIG_LOCATION)
    student_roster_178_df, student_roster_188_df = duckdb_data_extract_clean(config)   
    process_and_export_data(student_roster_178_df, student_roster_188_df, config)


if __name__ == "__main__":
    main()
    

from __future__ import print_function, division
import os
import time
from data_extraction import extract_pdf_data
from file_operation import check_and_combine_pdfs
from data_cleaning import clean_data
from utils import  load_configuration, export_results, post_processing_cleanup

CONFIGURATION_FILE = "LEAP/archived/config.json"

def main():

    config = load_configuration(CONFIGURATION_FILE)
    start_time = time.time()
    check_and_combine_pdfs(config)

    filename = os.path.join(config["Directory"], "cache", "combined_file.pdf")
    results = extract_pdf_data(filename, config)

    df2 = clean_data(results, config)
    export_results(df2, config)

    end_time = time.time()
    print(f"Results exported successfully. \nTotal running time: {end_time - start_time} seconds")

    post_processing_cleanup(config)

if __name__ == "__main__":
    main()
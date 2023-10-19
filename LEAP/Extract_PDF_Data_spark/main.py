import os
from multiprocessing import cpu_count
import fitz
from pyspark.sql import SparkSession
import time
import sys 
#import ExtractPDFDataTools 
from ExtractPDFDataTools.data_processing import process_spark_dataframe, process_file

import logging

CONFIGURATION_FILE = r"E:\Github\LDOE_Projects\LEAP\config.json"
PDF_DIRECTORY = r"E:\testing\pdf"
OUTPUT_PARQUET_FILE_PATH = r"E:\testing\Output\Report_Readin_2023_ELA_v2.parquet"
OUTPUT_CSV_FILE_PATH = r"E:\testing\Output\Report_Readin_2023_ELA.csv"
LOG_DIR = r"E:\testing\log"


if __name__ == "__main__":
    
    print(sys.path)

    # Initialize Spark session
    spark = SparkSession.builder.master("local[*]").appName("PDFDataExtraction").getOrCreate()
    sc = spark.sparkContext
    #sc.addPyFile("E:/Github/LDOE_Projects/LEAP/Extract_PDF_Data_spark/data_processing.py")

    # Setup logging
    setup_logging(LOG_DIR)

    # Load the config file
    config = load_config(CONFIGURATION_FILE)

    # Get the configuration parameters
    coordinate_mappings = config["coordinate_mappings"]
    Variable_List = config["Variable_List"]
    Subjects_in_Headlines = config["Subjects_in_Headlines"]
    Report_Type = config["Report_Type"]
    SUBJECT = config["SUBJECT"]

    start_time = time.time()

    # specify the file to be deleted
    filepath = os.path.join(PDF_DIRECTORY, "cache", "combined_file.pdf")

    combine_pdfs(PDF_DIRECTORY, Subjects_in_Headlines, Report_Type)
    
    try:
        filename = os.path.join(PDF_DIRECTORY, "cache", "combined_file.pdf")
        doc = fitz.open(filename)
    except FileNotFoundError:
        logging.error(f"The file {filename} does not exist, cannot open it.")
        sys.exit(1)
    except PermissionError:
        logging.error(f"Permission denied when accessing the file {filename}.")
        sys.exit(1)
    except Exception as e:
        logging.exception("An unexpected error occurred while opening the file.")
        sys.exit(1)

    # Set up core numbers for local machine
    num_pages = len(doc)
    num_cores = min(cpu_count(), num_pages)  # or however many cores you want to utilize
    pages_per_core = num_pages // num_cores
    
    # Create a list of page number ranges
    ranges = [
        (i, min(i + pages_per_core, num_pages))
        for i in range(0, num_pages, pages_per_core)
    ]

    # Create an RDD of page number ranges
    page_number_ranges = sc.parallelize(ranges)

    # Use the map function to process each page
    results = page_number_ranges.flatMap(
        lambda page_range: process_file(page_range, filename, coordinate_mappings)
    ).collect()

    # Close the PDF
    try:
        doc.close()
    except Exception as e:
        logging.exception("An error occurred while closing the file.")

    # Convert the results into a DataFrame
    df2 = spark.createDataFrame(results)

    df2 = process_spark_dataframe(df2, SUBJECT)

    # Export the results
    try:
        df2.write.mode("overwrite").parquet(OUTPUT_PARQUET_FILE_PATH)
        df2.write.mode("overwrite").csv(OUTPUT_CSV_FILE_PATH, header=True)
    except PermissionError:
        logging.error("Permission denied when writing to the destination.")
        sys.exit(1)
    except IOError as e:
        logging.exception("An I/O error occurred while writing to file.")
        sys.exit(1)
    except Exception as e:
        logging.exception("An unexpected error occurred while writing to file.")
        sys.exit(1)

    # Get the end time
    end_time = time.time()
    # Print the running time
    print(
        f"Results exported successfully. \nTotal running time: {end_time - start_time} seconds"
    )

    # specify the file to be deleted
    filepath = os.path.join(PDF_DIRECTORY, "cache", "combined_file.pdf")

    # Before deleting, ask for user confirmation
    confirm = input(f"Do you want to delete the cached file {filepath}? (y/n) \n")
    logging.info(f"User selected: '{confirm}'")
    # If the user confirms, delete the file
    if confirm.lower() == "y":
        try:
            delete_file(filepath)
        except FileNotFoundError:
            logging.error(f"The file {filepath} does not exist, cannot delete it.")
        except PermissionError:
            logging.error(f"Permission denied when deleting the file {filepath}.")
        except Exception as e:
            logging.exception("An unexpected error occurred while deleting the file.")
    else:
        print("File not deleted.")

    # Close the Spark session
    try:
        spark.stop()
    except Exception as e:
        logging.exception(f"An error occurred while closing the Spark session: {e}")

    logging.info("Script finished.")
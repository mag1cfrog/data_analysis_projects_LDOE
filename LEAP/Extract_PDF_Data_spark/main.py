import os
import time
import logging
from multiprocessing import cpu_count
import fitz
from pyspark.sql import SparkSession
from PDFDataExtractionTools.data_processing import process_spark_dataframe, process_file
from PDFDataExtractionTools.file_operations import delete_file, combine_pdfs
from utils import setup_logging, load_config, handle_file_exceptions



CONFIGURATION_FILE = "LEAP\\Extract_PDF_Data_spark\\config.json"


def initialize_spark_session():
    """Initialize and return a Spark session."""
    spark = SparkSession.builder.master("local[*]").appName("PDFDataExtraction").getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def setup_logging_and_load_config(config_file):
    """Set up logging and load the configuration file."""
    config = load_config(config_file)
    setup_logging(config['LOG_DIR'])
    return config


def combine_and_validate_pdfs(config):
    """Combine PDFs and ensure the combined file is accessible."""
    combine_pdfs(config['PDF_DIRECTORY'], config["Subjects_in_Headlines"], config["Report_Type"])
    filename = os.path.join(config['PDF_DIRECTORY'], "cache", "combined_file.pdf")
    try:
        doc = fitz.open(filename)
    except Exception as e:
        handle_file_exceptions(e, filename)
    return doc


def process_pdf_pages(doc, sc, config):
    """Divide the workload and process the pages in the PDF document."""
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
        lambda page_range: process_file(page_range, doc, config["coordinate_mappings"])
    ).collect()

    # Close the PDF
    try:
        doc.close()
    except Exception as e:
        logging.exception(f"An error occurred while closing the file: {e}.")

    return results


def export_results(spark, results, config):
    """Convert results to DataFrame and export them."""
    df = spark.createDataFrame(results)
    df = process_spark_dataframe(df, config["SUBJECT"])

    try:
        df.write.mode("overwrite").parquet(config['OUTPUT_PARQUET_FILE_PATH'])
        df.write.mode("overwrite").csv(config['OUTPUT_CSV_FILE_PATH'], header=True)
    except PermissionError:
        logging.error("Permission denied when writing to the destination.")
    except IOError as e:
        logging.exception("An I/O error occurred while writing to file.")
    except Exception as e:
        logging.exception("An unexpected error occurred while writing to file.")


def cleanup_and_close(spark, config):
    """Handle cache file deletion and close Spark session."""
    filepath = os.path.join(config['PDF_DIRECTORY'], "cache", "combined_file.pdf")
    confirm = input(f"Do you want to delete the cached file {filepath}? (y/n) \n")
    logging.info(f"User selected: '{confirm}'")

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

    try:
        spark.stop()
    except Exception as e:
        logging.exception(f"An error occurred while closing the Spark session: {e}")


def main():
    """Main function """
    spark, sc = initialize_spark_session()
    config = setup_logging_and_load_config(CONFIGURATION_FILE)

    start_time = time.time()

    doc = combine_and_validate_pdfs(config)
    results = process_pdf_pages(doc, sc, config)
    export_results(results, config)

    end_time = time.time()
    print(f"Results exported successfully. \nTotal running time: {end_time - start_time} seconds")

    cleanup_and_close(spark, config)

    print("Script finished.")


if __name__ == "__main__":
    
    main()
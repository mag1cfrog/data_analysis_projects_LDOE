from __future__ import print_function, division
import os

os.environ["NUMEXPR_MAX_THREADS"] = "16"
import re
import time
import fitz
from pdfrw import PdfReader, PdfWriter
from rich.progress import Progress
import json
from multiprocessing import cpu_count
from pyspark.sql import SparkSession, Row
import logging
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from pyspark import SparkContext as sc
import sys

CONFIGURATION_FILE = r"E:\Github\LDOE_Projects\LEAP\config.json"
PDF_DIRECTORY = r"E:\testing\pdf"
OUTPUT_PARQUET_FILE_PATH = r"E:\testing\Output\Report_Readin_2023_ELA_v2.parquet"
OUTPUT_CSV_FILE_PATH = r"E:\testing\Output\Report_Readin_2023_ELA.csv"
LOG_DIR = r"E:\testing\log"


# Data Extraction
def extract_data(page, keyword, replace_list=None):
    """
    Extracts data from the page based on the keyword and optional replacements.

    :param page: The page from which to extract data.
    :param keyword: The keyword based on which data is extracted.
    :param replace_list: A list of tuples with replacement rules.
    :return: Extracted data as a string.
    """
    words = get_words(page, keyword)
    if words and replace_list:
        for old, new in replace_list:
            words = words.replace(old, new)
    return words.strip() if words else words


def get_words(page, name):
    """
    Gets words from the page based on the name (coordinates).

    :param page: The page from which to get words.
    :param name: The name which maps to specific coordinates on the page.
    :return: Words as a string.
    """
    rect = fitz.Rect(
        coordinate_mappings[name]
    )  # Assuming coordinate_mappings is globally accessible or passed as an argument
    words = page.get_textbox(rect)
    return words


# Data Processing
def parse_percentages(words):
    """
    Parses percentages from the extracted words.

    :param words: Extracted words that may contain percentages.
    :return: A list of percentages.
    """
    return re.findall(r"(≥?≤?\d+%|NR|N/A)", words) if words else None


def process_achievement_levels(page, levels):
    """
    Processes achievement levels and extracts corresponding data.

    :param page: The page from which to extract data.
    :param levels: A list of achievement levels to process.
    :return: A dictionary with processed data for each level.
    """
    data = {}
    for level in levels:
        words = get_words(page, f"Percent_Achievement_Level_{level}")
        percentages = parse_percentages(words)
        if len(percentages) == 3:
            (
                data[f"{level}_School_Percentage"],
                data[f"{level}_School_System_Percentage"],
                data[f"{level}_State_Percentage"],
            ) = percentages
        else:
            print(
                f"Unexpected number of percentages for {level} in file {filename} on page {page.number + 1}, text: {words}"
            )
            print(len(percentages))
    return data


# Page Processing
def process_page(page, coordinate_mappings):
    """
    Processes a single page and extracts all necessary data.

    :param page: The page to process.
    :param coordinate_mappings: Dictionary containing coordinate mappings.
    :return: A dictionary containing processed data from the page.
    """

    def extract_and_clean_data(field_name, replace_patterns=None):
        words = get_words(page, field_name)
        if words and replace_patterns:
            for old, new in replace_patterns:
                words = words.replace(old, new)
        return words.strip() if words else None

    replace_score = [("\n", " "), ("SCORE", "")]
    replace_level = [("\n", " "), ("LEVEL", "")]
    replace_achievement_level = [("\n", " ")]

    data = {"Page_Number": page.number}

    # Process achievement levels
    levels = ["Advanced", "Mastery", "Basic", "Approaching_Basic", "Unsatisfactory"]
    data.update(process_achievement_levels(page, levels))

    # Extract other data
    # Title Section
    (
        data["Report_Title"],
        data["Report_Subject"],
        data["Report_Season_Year"],
    ) = get_words(page, "Report_Title_Section").split("\n")

    # Using the extract_and_clean_data function
    data["Personal_Information"] = extract_and_clean_data("Personal_Information")

    for term in ["Score", "Level", "Achievement_Level"]:
        # Dynamically get the replacement list based on the term
        replacements = eval(
            f"replace_{term.lower()}"
        )  # This retrieves the value of the variable with the name 'replace_score', 'replace_level', or 'replace_achievement_level'

        # Construct field names dynamically and process them
        for field in [
            f"Student_Performance_{term}",
            f"School_System_Average_{term}",
            f"State_Average_{term}",
        ]:
            data[field] = extract_and_clean_data(field, replacements)

    # Check if the report is voided
    not_voided = data["Student_Performance_Score"] != "*"
    data["If_Voided"] = not not_voided

    columns = [
        "Reading_Performance_Achievement_Level",
        "Literary_Text_Achievement_Level",
        "Informational_Text_Achievement_Level",
        "Vocabulary_Achievement_Level",
        "Reading_Performance_Achievement_Level_State_Percentages",
        "Writing_Performance_Achievement_Level",
        "Writing_Performance_Achievement_Level_State_Percentages",
        "Written_Expression_Achievement_Level",
        "Knowledge&Use_of_Language_Conventions",
    ]

    for col in columns:
        data[col] = get_words(page, col) if not_voided else None

    return data


# Document Processing
def process_file(page_range, filename, coordinate_mappings):
    """
    Processes a range of pages in the document.

    :param page_range: A tuple containing the start and end of the page range.
    :param filename: The document to process.
    :param coordinate_mappings: Dictionary containing coordinate mappings.
    :return: A list of results with data extracted from each page.
    """
    start, end = page_range
    doc = fitz.open(filename)
    results = []

    for page_num in range(start, end):
        page = doc[page_num]
        results.append(process_page(page, coordinate_mappings))
    doc.close()
    return results


# Combine PDFs
def combine_pdfs(PDF_DIRECTORY, Subjects_in_Headlines, Report_Type):
    """
    Combines PDFs in the directory into a single PDF file.
    :param PDF_DIRECTORY: The directory where the PDF files are stored.
    :param Subjects_in_Headlines: The list of subjects to be included in the headlines.
    :param Report_Type: The type of report to be combined.
    """
    start_time = time.time()

    # Get a list of all the PDF files in the directory
    pdf_files = [
        f
        for f in os.listdir(PDF_DIRECTORY)
        if f.endswith(".pdf")
        and any(Subject in f for Subject in Subjects_in_Headlines)
        and Report_Type in f
    ]

    # Create a new PDF writer object
    writer = PdfWriter()

    # Create a progress bar with Rich
    with Progress() as progress:
        task = progress.add_task("[red]Combining PDFs...", total=len(pdf_files))

        # Iterate over the PDF files
        for filename in pdf_files:
            # Update the progress bar
            progress.update(
                task, advance=1, description=f"[green]Processing {filename}..."
            )

            # Open the PDF file
            reader = PdfReader(os.path.join(PDF_DIRECTORY, filename))

            # Append the pages to the new PDF file
            writer.addpages(reader.pages)

    # Create the cache directory if it doesn't exist
    os.makedirs(os.path.join(PDF_DIRECTORY, "cache"), exist_ok=True)

    # Save the new PDF file
    writer.write(os.path.join(PDF_DIRECTORY, "cache", "combined_file.pdf"))

    # Get the end time
    end_time = time.time()

    # Print the running time
    print(f"Running time: {end_time - start_time} seconds")


if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder.appName("PDFDataExtraction").getOrCreate()
    sc = spark.sparkContext

    # Delete the file if it exists
    def delete_file(filepath):
        # Check if the file exists
        if os.path.exists(filepath):
            # Delete the file
            os.remove(filepath)
            print(f"The cached file has been deleted.")
        else:
            print(f"File {filepath} does not exist.")

    # Initialize logger
    # Get the current time, which will be used in the log file name
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # Construct the log file path
    log_file_path = os.path.join(LOG_DIR, f"log_{current_time}.txt")

    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # If you also want to see logs in the console, you can add a stream handler:
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console.setFormatter(formatter)
    logging.getLogger("").addHandler(console)

    # Test the logger
    logging.info("Script started.")

    # Load the config file
    with open(CONFIGURATION_FILE, "r") as f:
        config = json.load(f)

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

    # Old syntax for extracting Student_Name
    # Extract everything after "Student:" up to the newline
    df2 = df2.withColumn(
        "Student_Name",
        F.regexp_extract(F.col("Personal_Information"), r"Student: (.*)\n", 1),
    )

    # Handle the special case where 'Grade:' appears inside the student name
    df2 = df2.withColumn("Student_Name", F.split(F.col("Student_Name"), "Grade:")[0])

    # Trim any whitespace
    df2 = df2.withColumn("Student_Name", F.trim(F.col("Student_Name")))

    # Vectorized extraction for the other fields
    patterns = {
        "Grade": r"Grade: (.*?)\n",
        "Report_Date": r"Report Date: (.*?)\n",
        "LASID": r"LASID: (.*?)\n",
        "School": r"School: (.*?)\n",
        "School_System": r"School System: (.*?)(?:\n|$)",
        "DoB": r"Date of Birth: (.*?)\n",
    }

    # Extract the information using the patterns and store in respective columns
    for column, pattern in patterns.items():
        df2 = df2.withColumn(
            column, F.regexp_extract(df2["Personal_Information"], pattern, 1)
        )

    # Regulate the data in school column
    columns_to_strip = [
        "Student_Name",
        "School",
        "Grade",
        "LASID",
        "School_System",
        "DoB",
    ]

    for col in columns_to_strip:
        df2 = df2.withColumn(col, F.trim(df2[col]))

    columns_to_clean = [
        "Reading_Performance_Achievement_Level",
        "Literary_Text_Achievement_Level",
        "Informational_Text_Achievement_Level",
        "Vocabulary_Achievement_Level",
        "Written_Expression_Achievement_Level",
        "Knowledge&Use_of_Language_Conventions",
        "Writing_Performance_Achievement_Level",
    ]

    for col in columns_to_clean:
        df2 = df2.withColumn(
            col, F.regexp_replace(F.regexp_replace(df2[col], "«««", ""), "\n", " ")
        )

    def split_percentages(df, SUBJECT, source_col):
        target_cols = [
            f"{SUBJECT}_State_Percentage_{desc}"
            for desc in ["Strong", "Moderate", "Weak"]
        ]
        splits = F.split(F.trim(df[source_col]), "\n")
        for i, target_col in enumerate(target_cols):
            df = df.withColumn(target_col, splits[i])
        return df

    df2 = split_percentages(
        df2,
        "Reading_Performance_Achievement_Level",
        "Reading_Performance_Achievement_Level_State_Percentages",
    )
    df2 = split_percentages(
        df2,
        "Writing_Performance_Achievement_Level",
        "Writing_Performance_Achievement_Level_State_Percentages",
    )

    split_name = F.split(df2["Student_Name"], " ")
    df2 = df2.withColumn("Student_First_Name", split_name[0])
    df2 = df2.withColumn("Student_Last_Name", split_name[(F.size(split_name) - 1)])

    df2 = df2.withColumn(
        "Student_Middle_Initial",
        F.when(
            F.size(split_name) > 2,
            F.expr(
                'slice(split(Student_Name, " "), 2, size(split(Student_Name, " "))-2)'
            ),
        ).otherwise(F.array()),
    )
    df2 = df2.withColumn(
        "Student_Middle_Initial",
        F.when(F.expr('concat_ws(" ", Student_Middle_Initial)') == "", None).otherwise(
            F.expr('concat_ws(" ", Student_Middle_Initial)')
        ),
    )

    # Convert the 'DoB' column to datetime format
    df2 = df2.withColumn(
        "DoB", F.when(df2["DoB"] == "--/--/----", None).otherwise(df2["DoB"])
    )
    df2 = df2.withColumn("DoB", F.to_date(df2["DoB"], "MM/dd/yyyy").cast(DateType()))
    df2 = df2.withColumn("Summarized_DOB_Day", F.dayofmonth("DoB"))
    df2 = df2.withColumn("Summarized_DOB_Month", F.month("DoB"))
    df2 = df2.withColumn("Summarized_DOB_Year", F.year("DoB"))

    columns_to_drop = [
        "Reading_Performance_Achievement_Level_State_Percentages",
        "Writing_Performance_Achievement_Level_State_Percentages",
        "DoB",
    ]
    df2 = df2.drop(*columns_to_drop)

    df2 = df2.withColumnRenamed("Grade", "Summarized_Grade")
    df2 = df2.withColumnRenamed("Student_Performance_Score", f"Scale_Score_{SUBJECT}")

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

import logging
import json
import os
from datetime import datetime
import os
import time
from pdfrw import PdfReader, PdfWriter
from rich.progress import Progress
from pyspark.sql import functions as F
from pyspark.sql.types import DateType  
import re
from data_extraction import get_words
import fitz

def setup_logging(log_dir, level=logging.INFO):
    current_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file_path = os.path.join(log_dir, f"log_{current_time}.txt")

    logging.basicConfig(
        filename=log_file_path,
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler()
    console.setLevel(level)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    console.setFormatter(formatter)
    logging.getLogger("").addHandler(console)

def load_config(config_file):
    with open(config_file, "r") as f:
        return json.load(f)

def delete_file(filepath):
    if os.path.exists(filepath):
        os.remove(filepath)
        print(f"The cached file has been deleted.")
    else:
        print(f"File {filepath} does not exist.")

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

def process_spark_dataframe(df, SUBJECT):
    df = df.withColumn(
        "Student_Name",
        F.regexp_extract(F.col("Personal_Information"), r"Student: (.*)\n", 1),
    )

    # Handle the special case where 'Grade:' appears inside the student name
    df = df.withColumn("Student_Name", F.split(F.col("Student_Name"), "Grade:")[0])

    # Trim any whitespace
    df = df.withColumn("Student_Name", F.trim(F.col("Student_Name")))

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
        df = df.withColumn(
            column, F.regexp_extract(df["Personal_Information"], pattern, 1)
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
        df = df.withColumn(col, F.trim(df[col]))

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
        df = df.withColumn(
            col, F.regexp_replace(F.regexp_replace(df[col], "«««", ""), "\n", " ")
        )

    

    df = split_percentages(
        df,
        "Reading_Performance_Achievement_Level",
        "Reading_Performance_Achievement_Level_State_Percentages",
    )
    df = split_percentages(
        df,
        "Writing_Performance_Achievement_Level",
        "Writing_Performance_Achievement_Level_State_Percentages",
    )

    split_name = F.split(df["Student_Name"], " ")
    df = df.withColumn("Student_First_Name", split_name[0])
    df = df.withColumn("Student_Last_Name", split_name[(F.size(split_name) - 1)])

    df = df.withColumn(
        "Student_Middle_Initial",
        F.when(
            F.size(split_name) > 2,
            F.expr(
                'slice(split(Student_Name, " "), 2, size(split(Student_Name, " "))-2)'
            ),
        ).otherwise(F.array()),
    )
    df = df.withColumn(
        "Student_Middle_Initial",
        F.when(F.expr('concat_ws(" ", Student_Middle_Initial)') == "", None).otherwise(
            F.expr('concat_ws(" ", Student_Middle_Initial)')
        ),
    )

    # Convert the 'DoB' column to datetime format
    df = df.withColumn(
        "DoB", F.when(df["DoB"] == "--/--/----", None).otherwise(df["DoB"])
    )
    df = df.withColumn("DoB", F.to_date(df["DoB"], "MM/dd/yyyy").cast(DateType()))
    df = df.withColumn("Summarized_DOB_Day", F.dayofmonth("DoB"))
    df = df.withColumn("Summarized_DOB_Month", F.month("DoB"))
    df = df.withColumn("Summarized_DOB_Year", F.year("DoB"))

    columns_to_drop = [
        "Reading_Performance_Achievement_Level_State_Percentages",
        "Writing_Performance_Achievement_Level_State_Percentages",
        "DoB",
    ]
    df = df.drop(*columns_to_drop)

    df = df.withColumnRenamed("Grade", "Summarized_Grade")
    df = df.withColumnRenamed("Student_Performance_Score", f"Scale_Score_{SUBJECT}")

    return df


def split_percentages(df, SUBJECT, source_col):
        target_cols = [
            f"{SUBJECT}_State_Percentage_{desc}"
            for desc in ["Strong", "Moderate", "Weak"]
        ]
        splits = F.split(F.trim(df[source_col]), "\n")
        for i, target_col in enumerate(target_cols):
            df = df.withColumn(target_col, splits[i])
        return df

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


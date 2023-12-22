import re
import logging
import fitz
from utils import reset_log
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import cpu_count
import fitz


def initialize_document(filename, cpu):
    """Open a PDF document and calculate the segment size for processing."""
    doc = fitz.open(filename)
    num_pages = doc.page_count
    seg_size = int(num_pages / cpu + 1)
    return doc, seg_size


def extract_data(page, keyword, C_dict, replace_list=None):
    """Extract text from a specified area in a PDF page, with optional text replacements."""
    rect = fitz.Rect(C_dict[keyword])
    words = page.get_textbox(rect)
    if words and replace_list:
        for old, new in replace_list:
            words = words.replace(old, new)
        return words.strip()
    return words


def process_page_data(doc, idx, seg_size, num_pages, cpu, C_dict, filename):
    """Process a specific segment of pages from a PDF document."""
    data_list = []
    seg_from = idx * seg_size
    seg_to = min(seg_from + seg_size, num_pages)
    for i in range(seg_from, seg_to):
        page = doc[i]
        data = extract_page_data(page, i, C_dict, filename)
        data_list.append(data)
    return data_list


def extract_page_data(page, page_num, C_dict, filename):
    """Extract and compile data from a single page of a PDF document."""
    data = {'Page_Number': page_num}
    # Utility function to extract text and apply replacements
    def extract_text(keyword, replace_list=None):
        rect = fitz.Rect(C_dict[keyword])
        words = page.get_textbox(rect)
        if words and replace_list:
            for old, new in replace_list:
                words = words.replace(old, new)
        return words.strip() if words else None

    # Common replacements
    replace_score = [('\n', ' '), ('SCORE', '')]
    replace_level = [('\n', ' '), ('LEVEL', '')]
    replace_newline = [('\n', ' ')]

    # Extracting various data points
    data['Report_Title'], data['Report_Subject'], data['Report_Season_Year'] = extract_text('Report_Title_Section').split('\n')
    
    # Scores and Levels
    data['School_System_Average_Score'] = extract_text('School_System_Average_Score', replace_score)
    data['State_Average_Score'] = extract_text('State_Average_Score', replace_score)
    data['Student_Performance_Score'] = extract_text('Student_Performance_Score', replace_score)
    data['Student_Performance_Level'] = extract_text('Student_Performance_Level', replace_level)
    data['School_System_Average_Level'] = extract_text('School_System_Average_Level', replace_level)
    data['State_Average_Level'] = extract_text('State_Average_Level', replace_level)

    # Personal Information and Achievement Levels
    data['Personal_Information'] = extract_text('Personal_Information')
    data['Student_Performance_Achievement_Level'] = extract_text('Student_Achievement_Level', replace_newline)
    data['School_System_Average_Achievement_Level'] = extract_text('School_System_Average_Achievement_Level', replace_newline)
    data['State_Average_Achievement_Level_Fixed'] = extract_text('State_Average_Achievement_Level_Fixed', replace_newline)

    # Process achievement levels for 'Advanced', 'Mastery', etc.
    for level in ['Advanced', 'Mastery', 'Basic', 'Approaching_Basic', 'Unsatisfactory']:
        words = extract_text(f'Percent_Achievement_Level_{level}')
        percentages = re.findall(r'(≥?≤?\d+%|NR|N/A)', words) if words else None
        if percentages and len(percentages) == 3:
            data[f'{level}_School_Percentage'], data[f'{level}_School_System_Percentage'], data[f'{level}_State_Percentage'] = percentages
        else:
            print(f"Unexpected number of percentages for {level} in file {filename} on page {page.number + 1}, text: {words}")
            print(len(percentages))

    # Additional data based on the void status
    if data['Student_Performance_Score'] != '*':
        data['If_Voided'] = False
        # Additional categories
        columns = ['Reading_Performance_Achievement_Level', 'Literary_Text_Achievement_Level', 
                   'Informational_Text_Achievement_Level', 'Vocabulary_Achievement_Level', 
                   'Reading_Performance_Achievement_Level_State_Percentages', 'Writing_Performance_Achievement_Level',
                   'Writing_Performance_Achievement_Level_State_Percentages', 'Written_Expression_Achievement_Level', 
                   'Knowledge&Use_of_Language_Conventions']
        for col in columns:
            data[col] = extract_text(col)
    else:
        data['If_Voided'] = True

    return data


def extract_file_data(vector):
    """Process a segment of a PDF file for data extraction."""
    idx, cpu, filename, C_dict = vector
    doc, seg_size = initialize_document(filename, cpu)
    data_list = process_page_data(doc, idx, seg_size, doc.page_count, cpu, C_dict, filename)
    doc.close()
    return data_list


def extract_file_data_with_log(vector):
    """
    Process a segment of a PDF file and log the process.

    This function is an extension of the 'process_file_data' function with added logging capabilities.
    It initializes logging, records the start and completion of the process, and logs any exceptions that occur.
    The function processes a specified segment of a PDF file, identified by the input vector, 
    and returns the extracted data results. In case of an exception, an empty list is returned, 
    and the error is logged.

    Args:
    - vector (list): A list containing segment index, number of CPUs, filename, and a dictionary 
                     for text extraction coordinates. The first element (segment index) is also used for logging.

    Returns:
    - list: A list of dictionaries containing data extracted from each page in the segment. 
            Returns an empty list if an exception occurs.

    The function also creates or resets a log file at the beginning of the process, 
    where it logs the start, end, and any errors encountered during the processing of the file segment.
    """
    log_file = reset_log()
    logging.basicConfig(filename=log_file, level=logging.INFO)
    try:
        logging.info(f"Process {vector[0]} starting")
        result = process_file_data(vector)
        logging.info(f"Process {vector[0]} finished with {len(result)} results")

        return result
    except Exception as e:
        logging.error(f"Process {vector[0]} encountered an error: {e}")
        return []


def extract_pdf_data(filename, config):
    """
    Process data from the combined PDF using multiple processes.

    Args:
    - filename (str): Path to the combined PDF file.
    - config (dict): Configuration dictionary containing settings and parameters.

    Returns:
    - list: Aggregated results from processing each segment of the PDF.
    """
    doc = fitz.open(filename)
    cpu = min(cpu_count(), len(doc))
    doc.close()

    vectors = [(i, cpu, filename, config["coordinate_mappings"]) for i in range(cpu)]

    with ProcessPoolExecutor() as executor:
        results = list(executor.map(extract_file_data_with_log, vectors))

    flat_results = [item for sublist in results for item in sublist]
    return flat_results


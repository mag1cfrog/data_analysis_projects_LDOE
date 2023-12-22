from __future__ import print_function, division
import os
import pandas as pd
import re
import time
import fitz
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
from pdfrw import PdfReader, PdfWriter
import numpy as np
import json
from datetime import datetime


def process_file(vector):

    # recreate the arguments
    idx = vector[0]  # this is the segment number we have to process
    cpu = vector[1]  # number of CPUs
    filename = vector[2]  # document filename
    C_dict = vector[3]  # the matrix for rendering
    doc = fitz.open(filename)  # open the document
    num_pages = doc.page_count  # get number of pages

    # pages per segment: make sure that cpu * seg_size >= num_pages!
    seg_size = int(num_pages / cpu + 1)
    seg_from = idx * seg_size  # our first page number
    seg_to = min(seg_from + seg_size, num_pages)  # last page number
    data_list = []

    def extract_data(keyword, replace_list=None):
        words = get_words(keyword)
        if words and replace_list:
            for old, new in replace_list:
                words = words.replace(old, new)
            return words.strip()
        return words

    replace_score = [('\n', ' '), ('SCORE', '')]
    replace_level = [('\n', ' '), ('LEVEL', '')]
    replace_newline = [('\n', ' ')]

    for i in range(seg_from, seg_to):  # work through our page segment
        page = doc[i]
        # Initialize an empty dictionary to store the data            
        data = {}

        def get_words(name, page=page, C_dict=C_dict):
            rect = fitz.Rect(C_dict[name])
            words = page.get_textbox(rect)
            return words

        data['Page_Number'] = i
        # Extract the percentages for each achievement level
        for level in ['Advanced', 'Mastery', 'Basic', 'Approaching_Basic', 'Unsatisfactory']:

            words = get_words(f'Percent_Achievement_Level_{level}')
            # Use a regular expression to find all percentages in the text
            percentages = (re.findall(r'(≥?≤?\d+%|NR|N/A)', words) if words else None)
            # Check if the percentages were extracted correctly
            if len(percentages) == 3:
                # Store the percentages in the data dictionary
                data[f'{level}_School_Percentage'], data[f'{level}_School_System_Percentage'], data[f'{level}_State_Percentage'] = percentages
            else:
                print(f"Unexpected number of percentages for {level} in file {filename} on page {page.number + 1}, text: {words}")
                print(len(percentages))

        # Title Section 
        data['Report_Title'], data['Report_Subject'], data['Report_Season_Year'] = get_words('Report_Title_Section').split('\n')

        # Using the extract_data function
        data['School_System_Average_Score'] = extract_data('School_System_Average_Score', replace_score)
        data['State_Average_Score'] = extract_data('State_Average_Score', replace_score)
        data['Student_Performance_Score'] = extract_data('Student_Performance_Score', replace_score)
        data['Personal_Information'] = extract_data('Personal_Information')
        data['Student_Performance_Level'] = extract_data('Student_Performance_Level', replace_level)
        data['School_System_Average_Level'] = extract_data('School_System_Average_Level', replace_level)
        data['State_Average_Level'] = extract_data('State_Average_Level', replace_level)
        data['Student_Performance_Achievement_Level'] = extract_data('Student_Achievement_Level', replace_newline)
        data['School_System_Average_Achievement_Level'] = extract_data('School_System_Average_Achievement_Level', replace_newline)
        data['State_Average_Achievement_Level_Fixed'] = extract_data('State_Average_Achievement_Level_Fixed', replace_newline)

        # Check if the report is voided
        if data['Student_Performance_Score'] != '*':
            data['If_Voided'] = False
            # Subcategories

            # Reading_Performance_Achievement_Level            
            columns = ['Reading_Performance_Achievement_Level', 'Literary_Text_Achievement_Level', 'Informational_Text_Achievement_Level', 
                       'Vocabulary_Achievement_Level', 'Reading_Performance_Achievement_Level_State_Percentages', 'Writing_Performance_Achievement_Level',
                         'Writing_Performance_Achievement_Level_State_Percentages', 'Written_Expression_Achievement_Level', 'Knowledge&Use_of_Language_Conventions']

            for col in columns:
                data[col] = get_words(col)

#############################################################  
        else:
            data['If_Voided'] = True
#############################################################             
            
        # Append the data for this page to the list
        data_list.append(data)       

        # return the data for this file
    return data_list


def reset_log():
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    log_file = f'process_log_{timestamp}.txt'
    return log_file
    
import logging

def process_file_wrapper(vector):
    log_file = reset_log()
    logging.basicConfig(filename=log_file, level=logging.INFO)
    try:
        logging.info(f"Process {vector[0]} starting")
        result = process_file(vector)
        logging.info(f"Process {vector[0]} finished with {len(result)} results")

        return result
    except Exception as e:
        logging.error(f"Process {vector[0]} encountered an error: {e}")
        return []

def combine_pdfs(Directory, Subjects_in_Headlines, Report_Type):
    
    start_time = time.time()
    
    # Get a list of all the PDF files in the directory
    pdf_files = [f for f in os.listdir(Directory) if f.endswith('.pdf') and any(Subject in f for Subject in Subjects_in_Headlines) and Report_Type in f]

    # Create a new PDF writer object
    writer = PdfWriter()

    # Iterate over the PDF files
    for filename in tqdm(pdf_files, desc="Combining PDFs"):
        # Open the PDF file
        reader = PdfReader(os.path.join(Directory, filename))
       
        # Append the pages to the new PDF file
        writer.addpages(reader.pages)

    # Create the cache directory if it doesn't exist
    os.makedirs(os.path.join(Directory, 'cache'), exist_ok=True)

    # Save the new PDF file
    writer.write(os.path.join(Directory, 'cache', 'combined_file.pdf'))
    
    # Get the end time
    end_time = time.time()

    # Print the running time
    print(f"Running time: {end_time - start_time} seconds")

if __name__ == "__main__":

    # specify the directory where the PDF files are stored
    Directory = r'E:\testing\pdf\test'

    # Load the config file
    with open(r'E:\Github\LDOE_Projects\LEAP\config.json', 'r') as f:
        config = json.load(f)

    C_dict = config["C_dict"]
    Variable_List = config["Variable_List"]
    Subjects_in_Headlines = config['Subjects_in_Headlines']
    Report_Type = config['Report_Type']
   
    start_time = time.time()

    # Before combining, check if the combined file already exists
    if os.path.exists(os.path.join(Directory, 'cache', 'combined_file.pdf')):
        print("Combined file already exists.")
    else:
        combine_pdfs(Directory, Subjects_in_Headlines, Report_Type)

    filename = os.path.join(Directory, "cache", "combined_file.pdf")

    doc = fitz.open(filename)

    cpu = min(cpu_count(),len(doc))

    # Close the PDF
    doc.close()
    # make vectors of arguments for the processes
    vectors = [(i, cpu, filename, C_dict) for i in range(cpu)]
    print("Starting %i processes for '%s'." % (cpu, filename))

    # For CPU-bound tasks, use ProcessPoolExecutor
    # If it's an I/O-bound task, use ThreadPoolExecutor
    # This task is a CPU-bound task, so we use ProcessPoolExecutor
    with ProcessPoolExecutor() as executor:
        results = list(executor.map(process_file_wrapper, vectors))
     
    # Convert the results into a DataFrame
    flat_results = [item for sublist in results for item in sublist]
    # Convert the results into a PySpark DataFrame
    df2 = pd.DataFrame(flat_results) 


    # Old syntax for extracting Student_Name
    def extract_student_name(info):
        match = re.search(r'Student: (.*)\n', info)
        if match:
            student_name = match.group(1)
            if 'Grade:' in student_name:
                student_name = student_name.split('Grade:')[0]
            return student_name
        return None

    df2['Student_Name'] = df2['Personal_Information'].apply(extract_student_name)

    # Vectorized extraction for the other fields
    patterns = {
        'Grade': r'Grade: (.*?)\n',
        'Report_Date': r'Report Date: (.*?)\n',
        'LASID': r'LASID: (.*?)\n',
        'School': r'School: (.*?)\n',
        'School_System': r'School System: (.*?)(?:\n|$)',
        'DoB': r'Date of Birth: (.*?)\n'
    }

    for column, pattern in patterns.items():
        df2[column] = df2['Personal_Information'].str.extract(pattern)


    # Extract the information using the patterns and store in respective columns
    for column, pattern in patterns.items():
        df2[column] = df2['Personal_Information'].str.extract(pattern)

    # If you want to remove the original 'Personal_Information' column after extraction
    # df2.drop('Personal_Information', axis=1, inplace=True)


    # Regulate the data in school column
    columns_to_strip = ['Student_Name', 'School', 'Grade', 'LASID', 'School_System', 'DoB']

    for col in columns_to_strip:
        df2[col] = df2[col].str.strip()

    columns_to_clean = ['Reading_Performance_Achievement_Level', 'Literary_Text_Achievement_Level', 'Informational_Text_Achievement_Level', 'Vocabulary_Achievement_Level',
                        'Written_Expression_Achievement_Level', 'Knowledge&Use_of_Language_Conventions', 'Writing_Performance_Achievement_Level']

    for col in columns_to_clean:
        df2[col] = df2[col].str.replace('«««', '').str.replace('\n', ' ')

    def split_percentages(df, subject, source_col):
        target_cols = [f"{subject}_State_Percentage_{desc}" for desc in ['Strong', 'Moderate', 'Weak']]
        df[target_cols] = df[source_col].str.strip().str.split('\n', expand=True)
        return df

    df2 = split_percentages(df2, 'Reading_Performance_Achievement_Level', 'Reading_Performance_Achievement_Level_State_Percentages')
    df2 = split_percentages(df2, 'Writing_Performance_Achievement_Level', 'Writing_Performance_Achievement_Level_State_Percentages')

    # check for the presence of multiple subjects
    report_subjects = []
    report_subjects = list(df2['Report_Subject'].unique())

    if len(report_subjects) > 1:
        raise ValueError("More than one subject found in the files.")
    else:
        subject = report_subjects[0]

    if subject == 'English Language Arts':
        subject = 'ELA'

    # Split the 'Student_Name' column into three new columns 'Student_First_Name', 'Student_Last_Name', and 'Student_Middle_Initial'
    name_split = df2['Student_Name'].str.split(' ')
    df2['Student_First_Name'] = name_split.str[0]
    df2['Student_Last_Name'] = name_split.str[-1]
    df2['Student_Middle_Initial'] = name_split.str[1:-1].str.join(' ').str.strip()
    df2.loc[df2['Student_Middle_Initial'] == '', 'Student_Middle_Initial'] = np.nan

    # Convert the 'DoB' column to datetime format
    df2['DoB'] = df2['DoB'].replace('--/--/----', np.nan)
    df2['DoB'] = pd.to_datetime(df2['DoB'], errors='coerce')
    df2['Summarized_DOB_Day'] = df2['DoB'].dt.day
    df2['Summarized_DOB_Month'] = df2['DoB'].dt.month
    df2['Summarized_DOB_Year'] = df2['DoB'].dt.year

    # Split the 'School' column into two new columns 'School_Code' and 'School_Name'
    df2[['School_Code', 'School_Name']] = df2['School'].str.split(' ', n=1, expand=True)
    df2[['School_System_Code', 'School_System_Name']] = df2['School_System'].str.split(' ', n=1, expand=True)
    df2 = df2.drop(columns=['School_System', 'Reading_Performance_Achievement_Level_State_Percentages', 'Writing_Performance_Achievement_Level_State_Percentages', 'DoB', 'School'])

    df2 = df2.rename(columns={'Grade': 'Summarized_Grade', 'Student_Performance_Score': f'Scale_Score_{subject}'})

    df2.to_parquet('E:/testing/Output/Report_Readin_2023_ELA.parquet')
    df2.to_csv('E:/testing/Output/Report_Readin_2023_ELA.csv')

    end_time = time.time()
    # Print the running time
    print(f"Results exported successfully. \nTotal running time: {end_time - start_time} seconds")

    def delete_file(filepath):
        # Check if the file exists
        if os.path.exists(filepath):
            # Delete the file
            os.remove(filepath)
            print(f"The cached file has been deleted.")
        else:
            print(f"File {filepath} does not exist.")

    # specify the file to be deleted
    filepath = os.path.join(Directory, 'cache', 'combined_file.pdf')

    # Before deleting, ask for user confirmation
    confirm = input(f"Do you want to delete the cached file {filepath}? (y/n) \n")


    # If the user confirms, delete the file
    if confirm == 'y':
        delete_file(filepath)
    else:   
        print("File not deleted.")


    
    


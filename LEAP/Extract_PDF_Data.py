from __future__ import print_function, division
import os
import pandas as pd
import re
import time
import fitz
import winsound
import concurrent.futures
from tqdm import tqdm
#from process_file2 import process_file, process_file_wrapper
from multiprocessing import Pool, cpu_count
from multiprocessing import Value
from multiprocessing import Manager, Queue
from pdfrw import PdfReader, PdfWriter
import sys
from multiprocessing import Pool, cpu_count
import cProfile
import traceback
import numpy as np
from multiprocessing import Pool, cpu_count
from multiprocessing import Value, Queue
from datetime import datetime


def process_file(vector, counter):

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
            if words is not None:
                # Use a regular expression to find all percentages in the text
                percentages = re.findall(r'(≥?≤?\d+%|NR|N/A)', words)
                # Check if the percentages were extracted correctly
                if len(percentages) == 3:
                    # Store the percentages in the data dictionary
                    data[f'{level}_School_Percentage'], data[f'{level}_School_System_Percentage'], data[f'{level}_State_Percentage'] = percentages
                else:
                    print(f"Unexpected number of percentages for {level} in file {filename} on page {page.number + 1}, text: {words}")
                    print(len(percentages))

        # Title Section 
        data['Report_Title'], data['Report_Subject'], data['Report_Season_Year'] = get_words('Report_Title_Section').split('\n')

        # School System Average Score            
        words = get_words('School_System_Average_Score')
        if words is not None:           
            data['School_System_Average_Score'] = words.replace('\n', ' ').replace('SCORE', '') 

        # State average score            
        words = get_words('State_Average_Score')
        if words is not None:           
            data['State_Average_Score'] = words.replace('\n', ' ').replace('SCORE', '')          

        # Personal Informations    
        words = get_words('Personal_Information')

        if words is not None:
            data['Student_Name'] = re.search(r'Student: (.*)\n', words).group(1)
            if 'Grade:' in data['Student_Name']:
                data['Student_Name'] = data['Student_Name'].split('Grade:')[0]
            data['Grade'] = re.search(r'Grade: (.*)\n', words).group(1)
            data['Report_Date'] = re.search(r'Report Date: (.*)\n', words).group(1).strip()
            data['LASID'] = re.search(r'LASID: (.*)\n', words).group(1)
            data['School'] = re.search(r'School: (.*)\n', words).group(1)
            data['School_System'] = re.search(r'School System: (.*)', words).group(1)
            data['DoB'] = re.search(r'Date of Birth: (.*)\n', words).group(1)

        # Student Score            
        data['Student_Performance_Score'] = get_words('Student_Performance_Score').replace('\n', ' ').replace('SCORE', '').strip()
        # Use student score containing * or not to detect if voided
           
        # Student Level            
        words = get_words('Student_Performance_Level')
        if words is not None:           
            data['Student_Performance_Level'] = words.replace('\n', ' ').replace('LEVEL', '').strip()  

        #  School System Average Level            
        words = get_words('School_System_Average_Level')
        if words is not None:           
            data['School_System_Average_Level'] = words.replace('\n', ' ').replace('LEVEL', '') 


        # State Average Level            
        words = get_words('State_Average_Level')
        if words is not None:           
            data['State_Average_Level'] = words.replace('\n', ' ').replace('LEVEL', '')

        ### Achievement Levels
        words = get_words('Student_Achievement_Level')
        if words is not None:
            data['Student_Performance_Achievement_Level'] = words.replace('\n', ' ').strip()
        
        words = get_words('School_System_Average_Achievement_Level')
        if words is not None:
            data['School_System_Average_Achievement_Level'] = words.replace('\n', ' ').strip()

        data['State_Average_Achievement_Level_Fixed'] = get_words('State_Average_Achievement_Level_Fixed').replace('\n', ' ').strip()

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

def process_file_wrapper(args):
    log_file = reset_log()
    logging.basicConfig(filename=log_file, level=logging.INFO)
    vector, counter = args
    try:
        logging.info(f"Process {vector[0]} starting")
        result = process_file(vector, counter)
        logging.info(f"Process {vector[0]} finished with {len(result)} results")

        counter.value += len(result)
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
    writer.write(os.path.join(Directory, 'cache', 'combined.pdf'))
    
    # Get the end time
    end_time = time.time()

    # Print the running time
    print(f"Running time: {end_time - start_time} seconds")





if __name__ == "__main__":

    # specify the directory where the PDF files are stored
    Directory = r'E:\testing\pdf'

    # Initialize an empty DataFrame
    Variable_List = ['If_Voided', 'Report_Title', 'Report_Subject', 'Report_Type', 'Report_Year','Report_Season',
                    'Report_Date', 'Student_Name', 'LASID', 'DoB', 'Grade', 'School', 'School_System', 'Overview',
                    'State_Average_Score', 'State_Average_Level',  'State_Average_Achievement_Level',
                    'Student_Performance_Score', 'Student_Performance_Level', 'Student_Performance_Achievement_Level',
                    'School_System_Average_Score', 'School_System_Average_Level', 'School_System_Average_Achievement_Level',
                    'Overall_Student_Performance']


    # Coordinate Dictionary
    C_dict = {
        'If_Voided': (100.99799346923828, 240.17391967773438, 364.7249450683594, 304.9959716796875),
        'Report_Title_Section':(268.41949462890625, 24.466045379638672, 388.220458984375, 60.76609420776367),
        'Report_Title': (299.8905029296875, 24.466045379638672, 356.7494812011719, 35.46604537963867), 
        'Report_Subject': (307.530029296875, 37.11606979370117, 349.1100158691406, 48.11606979370117),
        'Report_Time':(297.1405029296875, 49.76609420776367, 359.4995422363281, 60.76609420776367),
        'Personal_Information': (36.0, 81.936432, 555.31427, 119.298805),
        'Overview': (36.0, 154.26290893554688, 573.7233276367188, 217.44131469726562),
        'Student_Performance_Score': (99.5, 272.1, 134.5139923095703, 304.9959716796875),
        'School_System_Average_Score': (437.9, 272.0903015136719, 472.9139709472656, 304.9959716796875),
        'State_Average_Score': (534.4, 272.0903015136719, 569.39404296875, 304.9959716796875),
        'Student_Performance_Level': (48.6, 276.0442199707031, 77.60700225830078, 306.5198974609375),
        'School_System_Average_Level': (392.8, 276.0442199707031, 421.7669982910156, 306.5198974609375),
        'State_Average_Level': (489.24, 276.0442199707031, 518.2470092773438, 306.5198974609375),
        'Overall_Student_Performance': (159.12, 240.17391967773438, 373.2570495605469, 311.8724365234375),
        'Achievement_Levels':(48.5, 313.1333312988281, 541.7593994140625, 332.9332275390625),
        'Achievement_Levels_Fixed':(48.5, 313.1333312988281, 561.5098876953125, 332.9332275390625),
        'Student_Achievement_Level':(48.5, 313.1333312988281, 138.4506072998047, 332.9332275390625),
        'School_System_Average_Achievement_Level':(431.37921142578125, 313.1333312988281, 475.8841552734375, 332.9332275390625),
        'State_Average_Achievement_Level':(522.6917724609375, 313.1333312988281, 567.1967163085938, 332.9332275390625),
        'State_Average_Achievement_Level_Fixed':(480, 313.1333312988281, 567.1967163085938, 332.9332275390625),
        'Investigate_Level': (47.3, 361.6931457519531, 109.95957946777344, 377.6131591796875),
        'Investigate_Rating': (121.3, 349.7764587402344, 565.6683959960938, 386.82415771484375),
        'Evaluate_Level': (47.3, 411.753173828125, 109.95957946777344, 427.6731872558594),
        'Evaluate_Level_Fixed':(47.3, 398, 109.9595718383789, 440.3182067871094),
        'Evaluate_Rating':(121.3, 399.83648681640625, 575.6884765625, 454.1341857910156),
        'Reason_Scientifically_Level': (47.3, 474.4582214355469, 109.95957946777344, 490.37823486328125),
        'Reason_Scientifically_Level_Fixed':(47.3, 468, 109.95957946777344, 521),
        'Reason_Scientifically_Rating': (121.3, 462.5415344238281, 572.3583984375, 499.5892333984375),
        'Percent_Achievement_Level_Advanced': (90.60675048828125, 669.9880981445312, 551.4429321289062, 678.0706176757812),
        'Percent_Achievement_Level_Mastery': (90.60675048828125, 683.6680908203125, 553.6669311523438, 691.6680908203125),
        'Percent_Achievement_Level_Basic': (90.60675048828125, 697.3480834960938, 553.6669311523438, 705.3480834960938),
        'Percent_Achievement_Level_Approaching_Basic': (90.60675048828125, 711.028076171875, 553.6669311523438, 719.028076171875),
        'Percent_Achievement_Level_Unsatisfactory': (90.60675048828125, 724.7080688476562, 553.6669311523438, 732.7080688476562),
        'Reading_Performance_Achievement_Level':(39.1879997253418, 387.57275390625, 94.01200103759766, 402.62274169921875),
        'Literary_Text_Achievement_Level':(39.57841873168945, 431.7209777832031, 94.40242004394531, 461.5955810546875),
        'Informational_Text_Achievement_Level':(39.57841491699219, 486.1009826660156, 94.40241241455078, 515.9755859375),
        'Vocabulary_Achievement_Level':(39.57841491699219, 540.4810180664062, 94.40241241455078, 570.3556518554688),
        'Reading_Performance_Achievement_Level_State_Percentages':(128.6118927001953, 407.6592102050781, 276.72808837890625, 417.1592102050781),
        'Writing_Performance_Achievement_Level':(317.1080017089844, 373.46807861328125, 371.9320373535156, 403.34271240234375),
        'Writing_Performance_Achievement_Level_State_Percentages':(404.37164306640625, 409.0992126464844, 552.4866943359375, 418.5992126464844),
        'Written_Expression_Achievement_Level':(317.33111572265625, 431.72100830078125, 372.15509033203125, 461.5956115722656),
        'Knowledge&Use_of_Language_Conventions':(317.33111572265625, 486.1009826660156, 372.15509033203125, 515.9755859375)
    }


    VS = pd.Series(Variable_List)

    Subjects_in_Headlines = ['ELA']


    Report_Type = 'StudentReport'
    
    start_time = time.time()

    combine_pdfs(Directory, Subjects_in_Headlines, Report_Type)

    filename = "cache/combined.pdf"
    filename = os.path.join(Directory, filename)

    doc = fitz.open(filename)
    total_pages = len(doc)
    cpu = min(cpu_count(),total_pages)
    # Close the PDF
    doc.close()
    # make vectors of arguments for the processes
    vectors = [(i, cpu, filename, C_dict) for i in range(cpu)]
    print("Starting %i processes for '%s'." % (cpu, filename))

    # Create a manager object
    manager = Manager()

    # Create a shared variable for the counter
    counter = manager.Value('i', 0)

    # Create a progress bar
    with Pool() as pool:
        results = []
        for result in pool.imap_unordered(process_file_wrapper, [(vector, counter) for vector in vectors]):
            results.append(result)
    
    
    # Convert the results into a DataFrame
    flat_results = [item for sublist in results for item in sublist]
    df2 = pd.DataFrame(flat_results)

    

    #Regulate the data in school column
    df2['Student_Name'] = df2['Student_Name'].str.strip()
    df2['School'] = df2['School'].str.strip()
    df2['Grade'] = df2['Grade'].str.strip()
    df2['LASID'] = df2['LASID'].str.strip()
    df2['School'] = df2['School'].str.strip()
    df2['School_System'] = df2['School_System'].str.strip()
    df2['DoB'] = df2['DoB'].str.strip()
    df2['Reading_Performance_Achievement_Level'] = df2['Reading_Performance_Achievement_Level'].str.replace('\n', ' ')
    df2['Literary_Text_Achievement_Level'] = df2['Literary_Text_Achievement_Level'].str.replace('«««', '').str.replace('\n', ' ')
    df2['Informational_Text_Achievement_Level'] = df2['Informational_Text_Achievement_Level'].str.replace('«««', '').str.replace('\n', ' ')
    df2['Vocabulary_Achievement_Level'] = df2['Vocabulary_Achievement_Level'].str.replace('«««', '').str.replace('\n', ' ')
    df2[['Reading_Performance_Achievement_Level_State_Percentage_Strong', 
        'Reading_Performance_Achievement_Level_State_Percentage_Moderate', 
        'Reading_Performance_Achievement_Level_State_Percentage_Weak']] = df2['Reading_Performance_Achievement_Level_State_Percentages'].str.strip().str.split('\n', expand=True)
    df2['Writing_Performance_Achievement_Level'] = df2['Writing_Performance_Achievement_Level'].str.replace('«««', '').str.replace('\n', ' ')
    df2[['Writing_Performance_Achievement_Level_State_Percentage_Strong', 
        'Writing_Performance_Achievement_Level_State_Percentage_Moderate', 
        'Writing_Performance_Achievement_Level_State_Percentage_Weak']] = df2['Writing_Performance_Achievement_Level_State_Percentages'].str.strip().str.split('\n', expand=True)
    df2['Written_Expression_Achievement_Level'] = df2['Written_Expression_Achievement_Level'].str.replace('«««', '').str.replace('\n', ' ')
    df2['Knowledge&Use_of_Language_Conventions'] = df2['Knowledge&Use_of_Language_Conventions'].str.replace('«««', '').str.replace('\n', ' ')
    report_subjects = []

    report_subjects = list(df2['Report_Subject'].unique())

#    if len(report_subjects) == 1:
#        subject = report_subjects[0]

#    if subject == 'English Language Arts':
#        subject = 'ELA'
    subject = 'ELA'

    # Split the 'Student_Name' column into two new columns
    
    # Split the 'Student_Name' column by spaces
    name_split = df2['Student_Name'].str.split(' ')

    # Extract the first name
    df2['Student_First_Name'] = name_split.str[0]

    # Extract the last name
    df2['Student_Last_Name'] = name_split.str[-1]

    # Extract the middle name or middle initial
    df2['Student_Middle_Initial'] = name_split.str[1:-1].str.join(' ').str.strip()

    # Handle cases where there's no middle name or middle initial
    df2.loc[df2['Student_Middle_Initial'] == '', 'Student_Middle_Initial'] = np.nan


    # Replace '--/--/----' with np.nan
    df2['DoB'] = df2['DoB'].replace('--/--/----', np.nan)

    # Convert the 'DoB' column to datetime format
    df2['DoB'] = pd.to_datetime(df2['DoB'], errors='coerce')

    # Create new columns for the day, month, and year
    df2['Summarized_DOB_Day'] = df2['DoB'].dt.day
    df2['Summarized_DOB_Month'] = df2['DoB'].dt.month
    df2['Summarized_DOB_Year'] = df2['DoB'].dt.year

    # Now you can drop the 'DoB' column as it's no longer needed
    df2 = df2.drop(columns='DoB')

    # Split the 'School' column into two new columns 'School_Code' and 'School_Name'
    df2[['School_Code', 'School_Name']] = df2['School'].str.split(' ', n=1, expand=True)
    df2 = df2.drop(columns='School')


    df2[['School_System_Code', 'School_System_Name']] = df2['School_System'].str.split(' ', n=1, expand=True)
    df2 = df2.drop(columns=['School_System', 'Reading_Performance_Achievement_Level_State_Percentages', 'Writing_Performance_Achievement_Level_State_Percentages'])

    df2 = df2.rename(columns={'Grade': 'Summarized_Grade'})
    df2 = df2.rename(columns={'Student_Performance_Score': f'Scale_Score_-_{subject}'})

    df2.to_parquet('E:/testing/Output/Report_Readin_2023_ELA.parquet')

    df2.to_csv('E:/testing/Output/Report_Readin_2023_ELA.csv')

    def delete_file(filepath):
        # Check if the file exists
        if os.path.exists(filepath):
            # Delete the file
            os.remove(filepath)
            print(f"File {filepath} has been deleted.")
        else:
            print(f"File {filepath} does not exist.")



    # specify the file to be deleted
    filepath = os.path.join(Directory, 'cache', 'combined.pdf')

    delete_file(filepath)

    end_time = time.time()
    # Print the running time
    print(f"Total running time: {end_time - start_time} seconds")
    


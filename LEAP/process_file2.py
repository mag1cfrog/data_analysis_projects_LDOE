from __future__ import print_function, division
import sys
import os
import time
from multiprocessing import Pool, cpu_count
import fitz
import os
import pandas as pd
import re
import time
import fitz
import winsound
import concurrent.futures
from tqdm import tqdm
import cProfile
import traceback
import numpy as np
from multiprocessing import Pool, cpu_count
from multiprocessing import Value, Queue
from tqdm import tqdm
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

        data['Page_Number'] = i
        # Extract the percentages for each achievement level
        for level in ['Advanced', 'Mastery', 'Basic', 'Approaching_Basic', 'Unsatisfactory']:
            rect = fitz.Rect(C_dict[f'Percent_Achievement_Level_{level}'])
            words = page.get_textbox(rect)
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

        # Student Score            
        rect = fitz.Rect(C_dict['Student_Performance_Score']) 
        words = page.get_textbox(rect)          
        data['Student_Performance_Score'] = words.replace('\n', ' ').replace('SCORE', '').strip()
        # Use student score containing * or not to detect if voided

        # Check if the report is voided            
        if data['Student_Performance_Score'] != '*':
            data['If_Voided'] = False

############################################################


            # Extract other data 

############################################################


            # Title Section
            rect = fitz.Rect(C_dict['Report_Title_Section']) 
            words = page.get_textbox(rect)
            data['Report_Title'], data['Report_Subject'], data['Report_Season_Year'] = words.split('\n')

            


            # Personal Informations            
            rect = fitz.Rect(C_dict['Personal_Information']) 
            words = page.get_textbox(rect)
            data['Student_Name'] = re.search(r'Student: (.*)\n', words).group(1)
            if 'Grade:' in data['Student_Name']:
                data['Student_Name'] = data['Student_Name'].split('Grade:')[0]
            data['Grade'] = re.search(r'Grade: (.*)\n', words).group(1)
#                        data['Report_Date'] = re.search(r'Report Date: (.*)\n', words).group(1).strip()
            data['LASID'] = re.search(r'LASID: (.*)\n', words).group(1)
            data['School'] = re.search(r'School: (.*)\n', words).group(1)
            data['School_System'] = re.search(r'School System: (.*)', words).group(1)
            data['DoB'] = re.search(r'Date of Birth: (.*)\n', words).group(1)


            # Overview            
#                    rect = fitz.Rect(C_dict['Overview']) 
#                    words = page.get_textbox(rect)
#                    data['Overview'] = words.replace('OVERVIEW\n', '').replace('\n', ' ')

           
            # School System Average Score            
            rect = fitz.Rect(C_dict['School_System_Average_Score']) 
            words = page.get_textbox(rect)
            if words is not None:           
                data['School_System_Average_Score'] = words.replace('\n', ' ').replace('SCORE', '') 


            # State average score            
            rect = fitz.Rect(C_dict['State_Average_Score']) 
            words = page.get_textbox(rect)           
            data['State_Average_Score'] = words.replace('\n', ' ').replace('SCORE', '') 

            # Student Level            
            rect = fitz.Rect(C_dict['Student_Performance_Level']) 
            words = page.get_textbox(rect)           
            data['Student_Performance_Level'] = words.replace('\n', ' ').replace('LEVEL', '').strip() 



            # School System Average Level            
            rect = fitz.Rect(C_dict['School_System_Average_Level']) 
            words = page.get_textbox(rect)
            if words is not None:           
                data['School_System_Average_Level'] = words.replace('\n', ' ').replace('LEVEL', '') 


            # State Average Level            
            rect = fitz.Rect(C_dict['State_Average_Level']) 
            words = page.get_textbox(rect)           
            data['State_Average_Level'] = words.replace('\n', ' ').replace('LEVEL', '')

            # Overall Student Performance            
#                    rect = fitz.Rect(C_dict['Overall_Student_Performance']) 
#                    words = page.get_textbox(rect)          
#                    data['Overall_Student_Performance'] = words.replace('OVERALL STUDENT PERFORMANCE\n', '').replace('\n', ' ')

            ### Achievement Levels

            rect = fitz.Rect(C_dict['Student_Achievement_Level']) 
            words = page.get_textbox(rect)
            data['Student_Performance_Achievement_Level'] = words.replace('\n', ' ').strip()

            
            
            rect = fitz.Rect(C_dict['School_System_Average_Achievement_Level']) 
            words = page.get_textbox(rect)
            data['School_System_Average_Achievement_Level'] = words.replace('\n', ' ').strip()

            
            rect = fitz.Rect(C_dict['State_Average_Achievement_Level_Fixed']) 
            words = page.get_textbox(rect)
            data['State_Average_Achievement_Level_Fixed'] = words.replace('\n', ' ').strip()

         
            

            # Subcategories

            # Reading_Performance_Achievement_Level            
            rect = fitz.Rect(C_dict['Reading_Performance_Achievement_Level']) 
            words = page.get_textbox(rect)           
            data['Reading_Performance_Achievement_Level'] = words

        
            rect = fitz.Rect(C_dict['Literary_Text_Achievement_Level']) 
            words = page.get_textbox(rect)           
            data['Literary_Text_Achievement_Level'] = words

        
            rect = fitz.Rect(C_dict['Informational_Text_Achievement_Level']) 
            words = page.get_textbox(rect)           
            data['Informational_Text_Achievement_Level'] = words


            rect = fitz.Rect(C_dict['Vocabulary_Achievement_Level']) 
            words = page.get_textbox(rect)           
            data['Vocabulary_Achievement_Level'] = words

            rect = fitz.Rect(C_dict['Reading_Performance_Achievement_Level_State_Percentages']) 
            words = page.get_textbox(rect)           
            data['Reading_Performance_Achievement_Level_State_Percentages'] = words

        
            rect = fitz.Rect(C_dict['Writing_Performance_Achievement_Level']) 
            words = page.get_textbox(rect)           
            data['Writing_Performance_Achievement_Level'] = words

        
            rect = fitz.Rect(C_dict['Writing_Performance_Achievement_Level_State_Percentages']) 
            words = page.get_textbox(rect)           
            data['Writing_Performance_Achievement_Level_State_Percentages'] = words


            rect = fitz.Rect(C_dict['Written_Expression_Achievement_Level']) 
            words = page.get_textbox(rect)           
            data['Written_Expression_Achievement_Level'] = words


            rect = fitz.Rect(C_dict['Knowledge&Use_of_Language_Conventions']) 
            words = page.get_textbox(rect)           
            data['Knowledge&Use_of_Language_Conventions'] = words





#############################################################  
        else:

            data['If_Voided'] = True
#############################################################             
            # Title Section
            rect = fitz.Rect(C_dict['Report_Title_Section']) 
            words = page.get_textbox(rect)
            data['Report_Title'], data['Report_Subject'], data['Report_Season_Year'] = words.split('\n')


            # Personal Informations            
            rect = fitz.Rect(C_dict['Personal_Information']) 
            words = page.get_textbox(rect)

            if words is not None:
                data['Student_Name'] = re.search(r'Student: (.*)\n', words).group(1).strip()
                if 'Grade:' in data['Student_Name']:
                    data['Student_Name'] = data['Student_Name'].split('Grade:')[0].strip()
                data['Grade'] = re.search(r'Grade: (.*)\n', words).group(1).strip()
#                        data['Report_Date'] = re.search(r'Report Date: (.*)\n', words).group(1).strip()
                data['LASID'] = re.search(r'LASID: (.*)\n', words).group(1).strip()
                data['School'] = re.search(r'School: (.*)\n', words).group(1).strip()
                data['School_System'] = re.search(r'School System: (.*)', words).group(1).strip()
                data['DoB'] = re.search(r'Date of Birth: (.*)\n', words).group(1).strip()

            # Overview            
#                    rect = fitz.Rect(C_dict['Overview']) 
#                    words = page.get_textbox(rect)
#                    if words is not None:
#                        data['Overview'] = words.replace('OVERVIEW\n', '').replace('\n', ' ')

            
            # School System Average Score            
            rect = fitz.Rect(C_dict['School_System_Average_Score']) 
            words = page.get_textbox(rect)
            if words is not None:           
                data['School_System_Average_Score'] = words.replace('\n', ' ').replace('SCORE', '') 


            # State average score            
            rect = fitz.Rect(C_dict['State_Average_Score']) 
            words = page.get_textbox(rect)
            if words is not None:           
                data['State_Average_Score'] = words.replace('\n', ' ').replace('SCORE', '') 

            # Student Level            
            rect = fitz.Rect(C_dict['Student_Performance_Level']) 
            words = page.get_textbox(rect)
            if words is not None:           
                data['Student_Performance_Level'] = words.replace('\n', ' ').replace('LEVEL', '').strip() 



            # School System Average Level            
            rect = fitz.Rect(C_dict['School_System_Average_Level']) 
            words = page.get_textbox(rect)
            if words is not None:           
                data['School_System_Average_Level'] = words.replace('\n', ' ').replace('LEVEL', '') 


            # State Average Level            
            rect = fitz.Rect(C_dict['State_Average_Level']) 
            words = page.get_textbox(rect)
            if words is not None:           
                data['State_Average_Level'] = words.replace('\n', ' ').replace('LEVEL', '')

            # Overall Student Performance            
#                    rect = fitz.Rect(C_dict['Overall_Student_Performance']) 
#                    words = page.get_textbox(rect)
#                    if words is not None:           
#                        data['Overall_Student_Performance'] = words.replace('OVERALL STUDENT PERFORMANCE\n', '').replace('\n', ' ')

            ### Achievement Levels


            rect = fitz.Rect(C_dict['Student_Achievement_Level']) 
            words = page.get_textbox(rect)
            data['Student_Performance_Achievement_Level'] = words.replace('\n', ' ').strip()

            
            rect = fitz.Rect(C_dict['School_System_Average_Achievement_Level']) 
            words = page.get_textbox(rect)
            data['School_System_Average_Achievement_Level'] = words.replace('\n', ' ').strip()

            
            rect = fitz.Rect(C_dict['State_Average_Achievement_Level']) 
            words = page.get_textbox(rect)
            data['State_Average_Achievement_Level'] = words.replace('\n', ' ').strip()
          


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




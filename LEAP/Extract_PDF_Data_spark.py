from __future__ import print_function, division
import os
import re
import time
import fitz
from concurrent.futures import ProcessPoolExecutor
from pdfrw import PdfReader, PdfWriter
import numpy as np
from tqdm import tqdm
import json
from multiprocessing import cpu_count
from pyspark.sql import SparkSession, Row
from datetime import datetime
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DateType
from pyspark import SparkContext as sc

def process_file(page_range, filename, C_dict):

    start, end = page_range
    doc = fitz.open(filename)  # open the document
    results = []

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

    for page_num in range(start, end):
        # Initialize an empty dictionary to store the data
        data = {}

        page = doc[page_num]
        
        def get_words(name, page=page, C_dict=C_dict):
                rect = fitz.Rect(C_dict[name])
                words = page.get_textbox(rect)
                return words
        
        

        data['Page_Number'] = page_num

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
            # Subcategories

            # Reading_Performance_Achievement_Level            
            columns = ['Reading_Performance_Achievement_Level', 'Literary_Text_Achievement_Level', 'Informational_Text_Achievement_Level', 
                        'Vocabulary_Achievement_Level', 'Reading_Performance_Achievement_Level_State_Percentages', 'Writing_Performance_Achievement_Level',
                            'Writing_Performance_Achievement_Level_State_Percentages', 'Written_Expression_Achievement_Level', 'Knowledge&Use_of_Language_Conventions']

            for col in columns:
                data[col] = None

    #############################################################             
            
        

        results.append(Row(**data))

    doc.close()
    return results

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

    # Initialize Spark session
    spark = SparkSession.builder.appName("PDFDataExtraction").getOrCreate()
    sc = spark.sparkContext

    # specify the directory where the PDF files are stored
    Directory = r'E:\testing\pdf'
    subject = 'ELA'
    
    def delete_file(filepath):
        # Check if the file exists
        if os.path.exists(filepath):
            # Delete the file
            os.remove(filepath)
            print(f"The cached file has been deleted.")
        else:
            print(f"File {filepath} does not exist.")

    
    # Load the config file
    with open(r'E:\Github\LDOE_Projects\LEAP\config.json', 'r') as f:
        config = json.load(f)

    C_dict = config["C_dict"]
    Variable_List = config["Variable_List"]
    Subjects_in_Headlines = config['Subjects_in_Headlines']
    Report_Type = config['Report_Type']
   
    start_time = time.time()

    # specify the file to be deleted
    filepath = os.path.join(Directory, 'cache', 'combined_file.pdf')
    
    combine_pdfs(Directory, Subjects_in_Headlines, Report_Type)

    filename = os.path.join(Directory, "cache", "combined_file.pdf")

    doc = fitz.open(filename)

    # Set up core numbers for local machine
    num_pages = len(doc)
    num_cores = min(cpu_count(), num_pages)  # or however many cores you want to utilize
    pages_per_core = num_pages // num_cores

    ranges = [(i, min(i+pages_per_core, num_pages)) for i in range(0, num_pages, pages_per_core)]

    # Create an RDD of page number ranges
    page_number_ranges = sc.parallelize(ranges)

    # Use the map function to process each page
    results = page_number_ranges.flatMap(lambda page_range: process_file(page_range, filename, C_dict)).collect()

    # Close the PDF
    doc.close()
    
    # Convert the results into a DataFrame
    df2 = spark.createDataFrame(results)


    # Old syntax for extracting Student_Name
    # Extract everything after "Student:" up to the newline
    df2 = df2.withColumn("Student_Name", F.regexp_extract(F.col("Personal_Information"), r'Student: (.*)\n', 1))

    # Handle the special case where 'Grade:' appears inside the student name
    df2 = df2.withColumn("Student_Name", F.split(F.col("Student_Name"), "Grade:")[0])

    # Trim any whitespace
    df2 = df2.withColumn("Student_Name", F.trim(F.col("Student_Name")))


    # Vectorized extraction for the other fields
    patterns = {
        'Grade': r'Grade: (.*?)\n',
        'Report_Date': r'Report Date: (.*?)\n',
        'LASID': r'LASID: (.*?)\n',
        'School': r'School: (.*?)\n',
        'School_System': r'School System: (.*?)(?:\n|$)',
        'DoB': r'Date of Birth: (.*?)\n'
    }

    # Extract the information using the patterns and store in respective columns
    for column, pattern in patterns.items():
        df2 = df2.withColumn(column, F.regexp_extract(df2['Personal_Information'], pattern, 1))


    # Regulate the data in school column
    columns_to_strip = ['Student_Name', 'School', 'Grade', 'LASID', 'School_System', 'DoB']

    for col in columns_to_strip:
        df2 = df2.withColumn(col, F.trim(df2[col]))

    columns_to_clean = ['Reading_Performance_Achievement_Level', 'Literary_Text_Achievement_Level', 'Informational_Text_Achievement_Level', 'Vocabulary_Achievement_Level',
                        'Written_Expression_Achievement_Level', 'Knowledge&Use_of_Language_Conventions', 'Writing_Performance_Achievement_Level']

    for col in columns_to_clean:
        df2 = df2.withColumn(col, F.regexp_replace(F.regexp_replace(df2[col], '«««', ''), '\n', ' '))

    def split_percentages(df, subject, source_col):
        target_cols = [f"{subject}_State_Percentage_{desc}" for desc in ['Strong', 'Moderate', 'Weak']]
        splits = F.split(F.trim(df[source_col]), '\n')
        for i, target_col in enumerate(target_cols):
            df = df.withColumn(target_col, splits[i])
        return df

    df2 = split_percentages(df2, 'Reading_Performance_Achievement_Level', 'Reading_Performance_Achievement_Level_State_Percentages')
    df2 = split_percentages(df2, 'Writing_Performance_Achievement_Level', 'Writing_Performance_Achievement_Level_State_Percentages')

    split_name = F.split(df2['Student_Name'], ' ')
    df2 = df2.withColumn('Student_First_Name', split_name[0])
    df2 = df2.withColumn('Student_Last_Name', split_name[(F.size(split_name)-1)])

    df2 = df2.withColumn('Student_Middle_Initial', F.when(F.size(split_name) > 2, F.expr('slice(split(Student_Name, " "), 2, size(split(Student_Name, " "))-2)')).otherwise(F.array()))
    df2 = df2.withColumn('Student_Middle_Initial', F.when(F.expr('concat_ws(" ", Student_Middle_Initial)') == '', None).otherwise(F.expr('concat_ws(" ", Student_Middle_Initial)')))

    # Convert the 'DoB' column to datetime format
    df2 = df2.withColumn('DoB', F.when(df2['DoB'] == '--/--/----', None).otherwise(df2['DoB']))
    df2 = df2.withColumn('DoB', F.to_date(df2['DoB'], 'MM/dd/yyyy').cast(DateType()))
    df2 = df2.withColumn('Summarized_DOB_Day', F.dayofmonth('DoB'))
    df2 = df2.withColumn('Summarized_DOB_Month', F.month('DoB'))
    df2 = df2.withColumn('Summarized_DOB_Year', F.year('DoB'))

    # Split the 'School' column into two new columns 'School_Code' and 'School_Name'
    split_school = F.split(df2['School'], ' ', 1)
    df2 = df2.withColumn('School_Code', split_school[0])
    df2 = df2.withColumn('School_Name', split_school[1])

    split_system = F.split(df2['School_System'], ' ', 1)
    df2 = df2.withColumn('School_System_Code', split_system[0])
    df2 = df2.withColumn('School_System_Name', split_system[1])

    columns_to_drop = ['School_System', 'Reading_Performance_Achievement_Level_State_Percentages', 
                   'Writing_Performance_Achievement_Level_State_Percentages', 'DoB', 'School']
    df2 = df2.drop(*columns_to_drop)

    df2 = df2.withColumnRenamed('Grade', 'Summarized_Grade')
    df2 = df2.withColumnRenamed('Student_Performance_Score', f'Scale_Score_{subject}')

    df2.write.mode("overwrite").parquet('E:/testing/Output/Report_Readin_2023_ELA_v2.parquet')

    df2.write.mode("overwrite").csv('E:/testing/Output/Report_Readin_2023_ELA.csv', header=True)

    end_time = time.time()
    # Print the running time
    print(f"Results exported successfully. \nTotal running time: {end_time - start_time} seconds")

    # specify the file to be deleted
    filepath = os.path.join(Directory, 'cache', 'combined_file.pdf')

    # Before deleting, ask for user confirmation
    confirm = input(f"Do you want to delete the cached file {filepath}? (y/n) \n")

    # If the user confirms, delete the file
    if confirm == 'y':
        delete_file(filepath)
    else:   
        print("File not deleted.")



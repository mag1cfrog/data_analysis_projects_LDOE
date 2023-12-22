import re
import numpy as np
import pandas as pd


def clean_data(results, config):
    """
    Extract relevant information from the results and perform data cleaning operations.

    Args:
    - results (list): List of dictionaries containing extracted data from the PDF.
    - config (dict): Configuration dictionary containing settings and parameters.

    Returns:
    - pd.DataFrame: A cleaned and processed Pandas DataFrame.
    """
    df = pd.DataFrame(results)

    # Define function for extracting student name
    def extract_student_name(info):
        match = re.search(r'Student: (.*)\n', info)
        if match:
            student_name = match.group(1)
            if 'Grade:' in student_name:
                student_name = student_name.split('Grade:')[0]
            return student_name
        return None

    df['Student_Name'] = df['Personal_Information'].apply(extract_student_name)

    # Define patterns for extracting various fields
    patterns = {
        'Grade': r'Grade: (.*?)\n',
        'Report_Date': r'Report Date: (.*?)\n',
        'LASID': r'LASID: (.*?)\n',
        'School': r'School: (.*?)\n',
        'School_System': r'School System: (.*?)(?:\n|$)',
        'DoB': r'Date of Birth: (.*?)\n'
    }

    # Extract information using the patterns
    for column, pattern in patterns.items():
        df[column] = df['Personal_Information'].str.extract(pattern)

    # Regulate the data in school column
    columns_to_strip = ['Student_Name', 'School', 'Grade', 'LASID', 'School_System', 'DoB']

    for col in columns_to_strip:
        df[col] = df[col].str.strip()

    columns_to_clean = ['Reading_Performance_Achievement_Level', 'Literary_Text_Achievement_Level', 'Informational_Text_Achievement_Level', 'Vocabulary_Achievement_Level',
                        'Written_Expression_Achievement_Level', 'Knowledge&Use_of_Language_Conventions', 'Writing_Performance_Achievement_Level']

    for col in columns_to_clean:
        df[col] = df[col].str.replace('«««', '').str.replace('\n', ' ')

    def split_percentages(df, subject, source_col):
        target_cols = [f"{subject}_State_Percentage_{desc}" for desc in ['Strong', 'Moderate', 'Weak']]
        df[target_cols] = df[source_col].str.strip().str.split('\n', expand=True)
        return df

    df = split_percentages(df, 'Reading_Performance_Achievement_Level', 'Reading_Performance_Achievement_Level_State_Percentages')
    df = split_percentages(df, 'Writing_Performance_Achievement_Level', 'Writing_Performance_Achievement_Level_State_Percentages')

    # check for the presence of multiple subjects
    report_subjects = []
    report_subjects = list(df['Report_Subject'].unique())

    if len(report_subjects) > 1:
        raise ValueError("More than one subject found in the files.")
    else:
        subject = report_subjects[0]

    if subject == 'English Language Arts':
        subject = 'ELA'

    # Split the 'Student_Name' column into three new columns 'Student_First_Name', 'Student_Last_Name', and 'Student_Middle_Initial'
    name_split = df['Student_Name'].str.split(' ')
    df['Student_First_Name'] = name_split.str[0]
    df['Student_Last_Name'] = name_split.str[-1]
    df['Student_Middle_Initial'] = name_split.str[1:-1].str.join(' ').str.strip()
    df.loc[df['Student_Middle_Initial'] == '', 'Student_Middle_Initial'] = np.nan

    # Convert the 'DoB' column to datetime format
    df['DoB'] = df['DoB'].replace('--/--/----', np.nan)
    df['DoB'] = pd.to_datetime(df['DoB'], errors='coerce')
    df['Summarized_DOB_Day'] = df['DoB'].dt.day
    df['Summarized_DOB_Month'] = df['DoB'].dt.month
    df['Summarized_DOB_Year'] = df['DoB'].dt.year

    # Split the 'School' column into two new columns 'School_Code' and 'School_Name'
    df[['School_Code', 'School_Name']] = df['School'].str.split(' ', n=1, expand=True)
    df[['School_System_Code', 'School_System_Name']] = df['School_System'].str.split(' ', n=1, expand=True)
    df = df.drop(columns=['School_System', 'Reading_Performance_Achievement_Level_State_Percentages', 'Writing_Performance_Achievement_Level_State_Percentages', 'DoB', 'School'])

    df = df.rename(columns={'Grade': 'Summarized_Grade', 'Student_Performance_Score': f'Scale_Score_{subject}'})

    return df
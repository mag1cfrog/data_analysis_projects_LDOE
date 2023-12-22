version = '0.0.6'
update_date = '2023/12/20'

import time
import json
from decimal import Decimal, ROUND_HALF_UP
import numpy as np
import xlwings as xw
import pandas as pd
import polars as pl



def load_json_config(config_location):
    with open(config_location, 'r') as file:
        return json.load(file)
    

def time_function(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"{func.__name__} executed in {end_time - start_time} seconds")
        return result
    return wrapper


def read_rpt_to_df(filepath: str) -> pd.DataFrame:
    """using the second line with all the dashes to get column specification to read the SQL Server rpt file
    
    """
    # get column specification using the second line with all the dashes
    count = 1
    for x in open(filepath, encoding='utf8'):
        cols = x.rstrip()  # remove newline character
        count += 1
        if count > 2:
            break

    # build colspecs list
    colspecs = []
    idx = 0
    for c in cols.split(' '):
        n = len(c)
        colspecs.append((idx, idx + n))
        idx += 1 + n

    df = pd.read_fwf(filepath, colspecs=colspecs, encoding='utf8', skiprows=[1])
    return df


def accurate_plus(parameter: list):
    """
    use decimal to calculate accurate plus
    :param parameter: list of parameters
    :return: accurate plus
    """
    if len(parameter) == 0:
        return 0
    else:
        return np.float64(sum([Decimal(str(i)) for i in parameter if pd.notnull(i)]))


def round_decimals(x, n=1):
    # Create the format string based on n, for example, '.1' for n=1, '.01' for n=2, and so on
    format_string = '.' + '0' * n
    rounded = Decimal(str(x)).quantize(Decimal(format_string), rounding=ROUND_HALF_UP)
    float_val = float(rounded)
    
    # Convert to Pandas 'Int64' type if it's an integer
    if float_val.is_integer():
        return np.int64(float_val)
    return float_val


def not_available(value):
    """Check if the component is available."""
    return value == 0 or value is None


def xw_excel_to_df(file_path, sheet_name=None, sheet_range=None, print_details=True):

    # start = time.time()

    app = xw.App(visible=False)
    book = xw.Book(file_path, read_only=True)

    dfs = []  # List to store DataFrames from each sheet

    if sheet_name is None:
        # Loop through all sheets in the workbook
        for sheet in book.sheets:
            if sheet_range is None:
                data = sheet.used_range.value
            else:
                data = sheet[sheet_range].value
            df = pd.DataFrame(data=data[1:], columns=data[0])
            dfs.append(df)
            if print_details:
                print(f"Processed sheet '{sheet.name}' with {len(df)} rows.")
    else:
        if sheet_range is None:
            data = book.sheets[sheet_name].used_range.value
        else:
            data = book.sheets[sheet_name][sheet_range].value
        df = pd.DataFrame(data=data[1:], columns=data[0])
        dfs.append(df)
        if print_details:
            print(f"Processed sheet '{sheet_name}' with {len(df)} rows.")

    # Concatenate all DataFrames into a single DataFrame
    final_df = pd.concat(dfs, ignore_index=True)

    book.close()
    app.quit()

    

    # end = time.time()

    # print(f'Reading time {end - start}s')

    return final_df


def read_fwf_pl(file_path: str, colspecs: list[tuple], columns: list[str], dtypes: list[pl.datatypes]=None):
    """
    Reads a fixed-width formatted file (FWF) into a Polars DataFrame.

    Parameters:
    - file_path (str): Path to the fixed-width file.
    - colspecs (list[tuple]): A list of tuples where each tuple contains the start 
      position (1-indexed) and the length of each column. For example, (1, 5) means 
      the column starts at the 1st character and has a length of 5 characters.
    - columns (list[str]): Names of the columns corresponding to the specifications 
      provided in colspecs.
    - dtypes (list[pl.datatypes], optional): Data types for each column. If not 
      provided, all columns are assumed to be of type pl.Utf8. The length of 
      dtypes should match the length of colspecs.

    Returns:
    - Polars DataFrame with the specified columns and data types.

    Example:
    widths = [(1, 10), (11, 10), (21, 10)]
    columns = ["ID", "Price", "Name"]
    dtypes = [pl.Utf8, pl.Float32, pl.Utf8]
    df = read_fwf_pl('path/to/fwf/file', widths, columns, dtypes)
    """
    """
    Reads a fixed-width formatted file (FWF) into a Polars DataFrame, adjusting for rows with varying lengths.

    Parameters:
    - file_path (str): Path to the fixed-width file.
    - colspecs (list[tuple]): List of tuples where each tuple contains the start 
      position (1-indexed) and the length of each column.
    - columns (list[str]): Names of the columns corresponding to colspecs.
    - dtypes (list[pl.datatypes], optional): Data types for each column.

    Returns:
    - Polars DataFrame with the specified columns and data types.

    Aware:
    - Due to unknown reason, some fwf files here have a inconsistent row length, I tried padding spaces at the start to make them consistent, and it worked for me.
    """
    # Read the file as a single-column DataFrame
    df = pl.read_csv(
        file_path, 
        separator="\n", 
        new_columns=["line_content"], 
        has_header=False
    )

    # Determine the maximum row length
    # print(df.select(pl.col('line_content').str.len_bytes()).max()['line_content'][0])
    max_row_length = df.select(pl.col('line_content').str.len_bytes()).max()['line_content'][0]
    
    # Pad each line to have the maximum length
    df = df.with_columns(
        pl.col('line_content').str.pad_start(max_row_length, ' ')
    )

    # Adjust colspecs for zero-based indexing
    adjusted_colspecs = [(start - 1, length) for start, length in colspecs]

    # Apply dtypes
    if dtypes is None:
        dtypes = [pl.Utf8] * len(colspecs)

    # Slice each line into columns
    column_information = zip(adjusted_colspecs, columns, dtypes)
    return df.select(
        [
            pl.col("line_content").str.slice(col_offset, col_len).cast(col_type).alias(col_name)
            for (col_offset, col_len), col_name, col_type in column_information
        ]
    )


def examine_row_lengths(file_path: str):
    """
    Reads a text file and returns a Polars DataFrame with the row number and the length of each row.

    Parameters:
    - file_path (str): Path to the text file.

    Returns:
    - Polars DataFrame with two columns: 'row_number' and 'row_length'.
    """
    # Read the file as a single-column DataFrame
    df = pl.read_csv(
        file_path, 
        separator="\n", 
        new_columns=["line_content"], 
        has_header=False
    )

    # Calculate the length of each row
    df = df.with_columns([
        pl.lit(df.height).alias("row_number"),
        pl.col("line_content").str.lengths().alias("row_length")
    ])

    # Add row number as a cumulative count
    df = df.with_column(pl.arange(1, df.height + 1).alias("row_number"))

    return df



def main():

    data = pd.read_parquet(r'c:\Hanbo Wang\ACT\Python\Match_NoMatch\LAP.parquet')
    retired_id_processor = RetiredIDProcessor(r'c:\Hanbo Wang\LAP\Retired LASIDs as of 10312022.xlsx')
    processed_data = retired_id_processor.process(data['LASID'].astype('float64').to_numpy())
    print(processed_data)


if __name__ == '__main__':
    # Usage examples
    # print(round_decimals(1.345, 2))  # rounds to 1.35
    # print(round_decimals(1.345, 1))  # rounds to 1.3
    # print(round_decimals(1.345))     # defaults to 1 decimal place: 1.3
    # print(round_decimals(1.045))

    # print(not_available(2-2))
    # print(accurate_plus([0.0, np.nan, np.nan, 0.0, 0.0, 3.75]))
    main()

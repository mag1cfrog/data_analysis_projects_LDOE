import os
from functools import reduce
import polars as pl
from utils import load_json_config, read_fwf_pl


# match no match 1050 file 

def read_lasid_mapping_to_pl(file_path: str, lasid_column=None):
    _, ext = os.path.splitext(file_path)

    if ext == '.csv':
        return pl.read_csv(file_path)
    elif ext == '.xlsx' or ext == '.xls':
        return pl.read_excel(file_path, schema_overrides={lasid_column: pl.Utf8})
    elif ext == '':
        raise TypeError("Only support mapping file in csv or xlsx, no file extension detected.")
    else:
        raise TypeError(f"Only support mapping file in csv or xlsx, detected file type {ext}")
    


def read_1050(file_1050_path, file_1050_columns, file_1050_colspecs):
    _, ext = os.path.splitext(file_1050_path)
    if ext == '.txt':
        # Read the fixed-width file
        file_1050 = read_fwf_pl(file_1050_path, file_1050_colspecs, file_1050_columns)
    elif ext == '.xlsx' or ext == '.xls':
        # Read the Excel file
        file_1050 = pl.read_excel(file_1050_path)
    elif ext == '.parquet':
        # Read the Parquet file
        file_1050 = pl.read_parquet(file_1050_path)
    
    return file_1050

def clean_lasid(file_1050, 
                file_1050_LASID_mapping, 
                file_1050_LASID_column, 
                mapping_new_LASID_column, 
                mapping_old_LASID_column, 
                alternative_LASID_column) -> pl.DataFrame:
    # Convert the 'ROWID' column to int64
    file_1050 = file_1050.with_columns(pl.col(file_1050_LASID_column).str.strip_chars().cast(pl.Float64).cast(pl.Int64))

    # file_1050_LASID_mapping = file_1050_LASID_mapping.with_columns(
    #     pl.col(mapping_old_LASID_column).cast(pl.Utf8).str.slice(0, 10).cast(pl.Int64).alias(mapping_old_LASID_column))

    # Merge the two dataframes on 'ROWID' and 'RecordID'
    file_1050_LASID = file_1050.join(file_1050_LASID_mapping, left_on=file_1050_LASID_column, right_on=mapping_old_LASID_column, how='left')

    # Create a new column 'LASID' based on conditions, then sort by ROWID
    file_1050_LASID = file_1050_LASID.with_columns(
                pl.when(pl.col(mapping_new_LASID_column).is_not_null())
                    .then(pl.col(mapping_new_LASID_column).cast(pl.Utf8))
                    .otherwise(pl.col(alternative_LASID_column))
                    .alias('LASID')
            ).sort(file_1050_LASID_column)

    return file_1050_LASID


def generate_voided_list(file_1050_LASID):
    # Generate VOIDFLAG column
    file_1050_LASID = file_1050_LASID.with_columns(
                pl.when(pl.col('STATE_Q10').is_in(['L', 'H']))
                    .then(pl.lit('Y'))
                    .otherwise(pl.lit(None))
                    .alias('VOIDFLAG')
            ).select('LASID', 'VOIDFLAG')


    voided_list = list(file_1050_LASID.filter(file_1050_LASID['VOIDFLAG'] == 'Y')['LASID'].cast(pl.Int64))
    return voided_list


def read_1050_clean_lasid_generate_voidlist(file_path, columns, colspecs, mapping_file_path,
                                            file_1050_LASID_column='ROWID', 
                                            mapping_new_LASID_column='LASID', 
                                            mapping_old_LASID_column='RecordID', 
                                            alternative_LASID_column='LASID_ACT1'
                                            ):
    # Read the fixed-width file
    file_1050 = read_1050(file_path, columns, colspecs)

    # print(file_path, columns, colspecs, mapping_file_path,
    #                                         file_1050_LASID_column, 
    #                                         mapping_new_LASID_column, 
    #                                         mapping_old_LASID_column, 
    #                                         alternative_LASID_column)
    # Read the CSV file
    file_1050_LASID_mapping = read_lasid_mapping_to_pl(mapping_file_path, mapping_new_LASID_column)

    file_1050_LASID = clean_lasid(file_1050, file_1050_LASID_mapping, file_1050_LASID_column, mapping_new_LASID_column, mapping_old_LASID_column, alternative_LASID_column)

    return generate_voided_list(file_1050_LASID)



def create_void_df(void_list, column_name):
    return pl.DataFrame({'LASID': list(set(void_list))}).with_columns(pl.lit(True).alias(column_name))


def la_files_to_void_df(config: dict):
 
    # Validate the configuration
    number_of_files = config["LA_files"]["number"]
    if not (len(config["LA_files"]["main_file_locations"]) == number_of_files and 
            len(config["LA_files"]["lasid_file_locations"]) == number_of_files and
            len(config["LA_files"]["extra_arguments"]) == number_of_files):
        raise ValueError("Configuration mismatch: The lengths of file lists and the specified number do not match.")

    # Read the file layout from config
    colspecs = [tuple(lst) for lst in config["la_main_file_layout"]['colspecs']]
    columns = config["la_main_file_layout"]['columns']

    # Process each file pair from the JSON configuration
    void_info = []
    for i in range(number_of_files):
        main_file_path = config["LA_files"]["main_file_locations"][i]
        lasid_file_path = config["LA_files"]["lasid_file_locations"][i]
        extra_args = config["LA_files"]["extra_arguments"][i] or {}

        # Generate the void list for each pair
        void_list = read_1050_clean_lasid_generate_voidlist(main_file_path, columns, colspecs, lasid_file_path, **extra_args)
        void_info.append((void_list, f'Voided_{i+1}'))

    # Generate DataFrames for each void list
    void_dfs = [create_void_df(void_list, column_name) for void_list, column_name in void_info]

    # Merge all DataFrames using outer join
    voids = reduce(lambda left, right: left.join(right, on='LASID', how='outer'), void_dfs)

    return voids


# For testing
def main():
    CONFIG_LOCATION = "C:\\Git\\LDOE\\ACT\\23\\match_nomatch\\config_match_no_match.json"
    config = load_json_config(CONFIG_LOCATION)
    print(la_files_to_void_df(config))


if __name__ == '__main__':
    main()
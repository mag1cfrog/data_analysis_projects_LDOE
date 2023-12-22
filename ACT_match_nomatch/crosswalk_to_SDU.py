import json
import os
import polars as pl
from utils import load_json_config

CONFIG_LOCATION = "C:\\Git\\LDOE\\ACT\\23\\match_nomatch\\config_match_no_match.json"

def read_column_setting_with_f_string(config: dict, dict_name: str)-> list:

    if 'list_of_variable_lists' not in config[dict_name]:
        raise TypeError("configuration error, list_of_variable_lists not found")
    else:
        total_list = []
        for one_list in config[dict_name]['list_of_variable_lists']:
            if 'type' not in config[dict_name][one_list]:
                raise TypeError("configuration error, type for this list not found")
            elif config[dict_name][one_list]['type'] == 'base':
                total_list += config[dict_name][one_list]['content']
            elif config[dict_name][one_list]['type'] == 'range':
                total_list += [f"{config[dict_name][one_list]['prefix']}{i}" for i in range(config[dict_name][one_list]['start'], config[dict_name][one_list]['end'])]
            else:
                raise TypeError("configuration error, type for this list is not base or range")
        
        return total_list
    

def read_and_clean_crosswalk(config: dict):
    # Specify the file path to old_new site code mapping path JSON file
    old_new_mapping_path = config['old_new_mapping_path']

    # Open the file and load its contents into a Python dictionary
    with open(old_new_mapping_path, 'r') as file:
        old_new_site_code_mapping_dict = json.load(file)

    crosswalk_file_path = config['crosswalk_file_path']

    crosswalk_df = pl.read_excel(crosswalk_file_path, schema_overrides={'SITECD': pl.Utf8, 'Organization_Code': pl.Utf8})

    crosswalk_df_2 = crosswalk_df.with_columns([
        pl.col('SITECD').replace(old_new_site_code_mapping_dict, default=pl.col('SITECD')).alias('SITECD')
        
    ]).rename({'SITECD': 'site_code'})

    precode_to_exclude_list = config['precode_to_exclude_list']

    crosswalk_df_3 = crosswalk_df_2.filter(~pl.col('site_code').is_in(precode_to_exclude_list))

    crosswalk_final_df = crosswalk_df_3.sort(
        by=['Organization_Code', 'site_code', 'Parent_Organization_Code'], descending=False
        ).unique(subset='site_code')
    
    return crosswalk_final_df


def join_crosswalk_with_student_roster_and_clean(config: dict, nov_lap_11: pl.DataFrame, student_roster_4: pl.DataFrame, crosswalk_final_df: pl.DataFrame):
    col_to_keep_SDU = config['col_to_keep_SDU']

    nov_lap_11_2 = nov_lap_11.rename({'DistrictCode':'district_code'}).select(col_to_keep_SDU).with_columns(
        pl.col('LASID').cast(pl.Int64)
    )

    act_nomatch_lap_grd11 = pl.concat(
        [student_roster_4.filter(pl.col('if_matched') != 'Y').select(col_to_keep_SDU), 
        nov_lap_11_2])

    SDU_total_roster = act_nomatch_lap_grd11.join(crosswalk_final_df, on='site_code', how='left')

    SDU_total_roster_2 = SDU_total_roster.filter(~SDU_total_roster['Organization_Code'].is_null())

    SDU_total_roster_2 = SDU_total_roster_2.with_columns([
        pl.when(pl.col('site_code').str.slice(0, 1).is_in(['5', '6', '7', '8', '9']))
        .then(pl.lit('X'))
        .otherwise(pl.lit('Y'))
        .alias('LEAOptInFlg'),  # LEAOptInFlg
        
        pl.when(pl.col('DOBYear') == '1900')
        .then(pl.col('DOBDay'))
        .otherwise(pl.col('DOBMonth') + pl.lit('/') + pl.col('DOBDay') + pl.lit('/') + pl.col('DOBYear'))
        .alias('Date of Birth') # Date of Birth
    ])
    return SDU_total_roster_2


def formatting_SDU(config, SDU_total_roster_2: pl.DataFrame):

    # List of new column names
    SDU_new_null_columns = read_column_setting_with_f_string(config, 'SDU_new_null_columns')

    SDU_new_fixed_content_columns = config['SDU_new_fixed_content_columns']

    SDU_rename_dict = config['SDU_rename_dict']

    col_to_keep_SDU_final = read_column_setting_with_f_string(config, 'col_to_keep_SDU_final')

    # Create expressions for each new null column
    SDU_null_column_exprs = [pl.lit(None).alias(col_name) for col_name in SDU_new_null_columns]

    # expression for fixed content columns
    SDU_fixed_column_exprs = [pl.lit(col_content).alias(col_name) for col_content, col_name in SDU_new_fixed_content_columns]

    # Add the new columns to the DataFrame
    SDU_df = SDU_total_roster_2.with_columns(
        SDU_null_column_exprs + SDU_fixed_column_exprs
        ).rename(SDU_rename_dict).select(col_to_keep_SDU_final)
    
    # Transform some data type of some specific columns
    SDU_df = SDU_df.with_columns(pl.col('State Student ID').cast(pl.Utf8).alias('State Student ID'))

    return SDU_df
 

def crosswalk_to_SDU(config: dict, nov_lap_11: pl.DataFrame, student_roster_4: pl.DataFrame):
    # Crosswalk part
    # Read and clean crosswalk file
    crosswalk_final_df = read_and_clean_crosswalk(config)
    # Join crosswalk with student level roster and then clean it
    SDU_total_roster_2 = join_crosswalk_with_student_roster_and_clean(config, nov_lap_11, student_roster_4, crosswalk_final_df)
    # Formatting the SDU data as requested
    SDU_df = formatting_SDU(config, SDU_total_roster_2)
    # Export final result to specific location
    SDU_file_path = os.path.join(config['SDU_output']['directory'], config['SDU_output']['filename'])
    SDU_df.write_excel(SDU_file_path, autofit=True)


   


# For testing
def main():
    config = load_json_config(CONFIG_LOCATION)

    print(read_column_setting_with_f_string(config, 'col_to_keep_SDU_final'))


if __name__ == '__main__':
    main()
import polars as pl


def read_school_list_to_df(config: dict):
    # School List
    # Define the file path
    schlist_file_path = config['schlist_file_path']

    # Define a function to parse each line
    def parse_line(line):
        return {
            'site_code': line[52:58].strip(),
            'asst_group_code': line[109:112].strip(),
            'district_name': line[953:1004].strip(),
            'site_name': line[1004:1055].strip(),
            'dis_report_code': line[1376:1382].strip()
        }

    # Read the file, apply the parsing function to each line, and convert to DataFrame
    with open(schlist_file_path, 'r', encoding='utf-8') as file:
        data = [parse_line(line) for line in file]

    dir_schlist = pl.DataFrame(data)
    return dir_schlist


def read_lap_file(config: dict, id_processor: object):
    # Reading the input file and creating the DataFrame NOVLAP_0
    lap_file_path = config['lap_file_path']

    columns_lap = config['columns_lap']

    nov_lap = pl.read_csv(lap_file_path, separator='|', has_header=False,  encoding='ISO-8859-1', new_columns=columns_lap, infer_schema_length=0)

    # TestType =5 is duplicated
    nov_lap = nov_lap.with_columns([pl.col('GradePlacement').str.strip_chars(),
                                                pl.col('TestType').str.strip_chars(),
                                                pl.col('SchoolCode').str.strip_chars(),
                                                (pl.col('DistrictCode').str.strip_chars() + pl.col('SchoolCode').str.strip_chars().str.zfill(3)).alias('site_code')
                                                ]).filter((nov_lap['TestType'] != '5') & (nov_lap['SchoolCode'] != '997'))

    nov_lap_2 = nov_lap.filter(nov_lap['GradePlacement'] == '12')
    nov_lap_11 = nov_lap.filter(nov_lap['GradePlacement'] == '11')

    # update LASIDs

    new_lasids = id_processor.process(nov_lap_2['LASID'].cast(pl.Float64).to_numpy())

    nov_lap_3 = nov_lap_2.with_columns([
        pl.Series('LASID', new_lasids).cast(pl.Int64).alias('LASID'),
        pl.col('DOBDay').str.zfill(2).alias('DOBDay')
        ])

    sitecode_to_exclude_list = config['sitecode_to_exclude_list']

    nov_lap_3 = nov_lap_3.filter(~pl.col('site_code').is_in(sitecode_to_exclude_list))

    return nov_lap_3, nov_lap_11


def read_act_file(config: dict, id_processor: object):
    # History ACT data
    ACT_test_location = config['ACT_test_location']
    Total_History_Test_Record = pl.read_parquet(ACT_test_location)

    Total_History_Test_Record.with_columns(pl.col('LASID_new').cast(pl.Float64)).select(pl.col('LASID_new'))

    history_test = Total_History_Test_Record.clone()

    history_test_2 = history_test.with_columns(
        pl.Series('LASID_new', id_processor.process(
            history_test['LASID_new'].cast(pl.Float64).to_numpy()
            )
            ))
    
    # Only use the records with a valid Score_Composite for history test records
    history_lasid_series = history_test_2.filter(history_test_2['Score_Composite'].is_not_nan())['LASID_new'].cast(pl.Float64)

    return history_test_2, history_lasid_series
import os
import polars as pl
import xlsxwriter
from initial_data_reading_preprocessing import read_school_list_to_df, read_lap_file, read_act_file
from la_files_processing import la_files_to_void_df


def files_input(config, id_processor):
    # School List
    dir_schlist = read_school_list_to_df(config)
    # Lap File
    nov_lap_3, nov_lap_11 = read_lap_file(config, id_processor)
    # ACT File
    history_test_2, history_lasid_series = read_act_file(config, id_processor)
    # Merge Lap File with School List
    student_roster_3 = merge_lap_with_school_list(nov_lap_3, dir_schlist)

    return student_roster_3, history_test_2, history_lasid_series, nov_lap_11


def export_match_nomatch(config, match_students_final_df, no_match_students_final_df):
    # Assuming no_match_students_final_df and match_students_final_df are your Polars dataframes
    match_no_match_output_location = os.path.join(config['output_match_no_match']['directory'], config['output_match_no_match']['filename'])

    with xlsxwriter.Workbook(match_no_match_output_location) as wb:
        # Write the first dataframe to the first sheet
        no_match_students_final_df.write_excel(workbook=wb, worksheet='NO_MATCH', autofit=True)

        # Write the second dataframe to the second sheet
        match_students_final_df.write_excel(workbook=wb, worksheet='MATCH', autofit=True)



def merge_lap_with_school_list(nov_lap_3, dir_schlist):

    student_roster = nov_lap_3.join(dir_schlist, left_on='site_code', right_on='site_code', how='left')

    student_roster_2 = student_roster.rename({'DistrictCode': 'district_code'})

    R36_mask = student_roster_2['dis_report_code'] == 'R36'

    student_roster_3 = student_roster_2.with_columns([
        pl.when(R36_mask)
        .then(pl.lit('R36'))
        .otherwise(pl.col('district_code'))
        .alias('district_code'),

        pl.when(R36_mask)
        .then(pl.lit('Orleans Parish'))
        .otherwise(pl.col('district_name'))
        .alias('district_name')
    ])

    district_info_updated_from_site_mask = (pl.col('district_code') > '069') & ~(pl.col('district_code').is_in(['R36', '502']))

    student_roster_3 = student_roster_3.with_columns([
        pl.when(district_info_updated_from_site_mask).then(pl.col('site_code')).otherwise(pl.col('district_code')).alias('district_code'),
        pl.when(district_info_updated_from_site_mask).then(pl.col('site_name')).otherwise(pl.col('district_name')).alias('district_name')
    ])

    student_roster_3 = student_roster_3.with_columns((student_roster_3['district_code'] + pl.lit('_') + student_roster_3['district_name']).alias('district_code_name') )

    return student_roster_3



def generate_match_nomatch(config, id_processor):
    # Read files
    student_roster_3, history_test_2, history_lasid_series, nov_lap_11 = files_input(config, id_processor)
    # Generate voids from LA files
    voids = la_files_to_void_df(config)
    # Create series for faster lookup
    total_lasid_series = student_roster_3['LASID']
    voids_lasid_series = voids['LASID']
    found_in_history_series = total_lasid_series.filter(total_lasid_series.is_in(history_lasid_series))

    found_in_voids_series = total_lasid_series.filter(
        total_lasid_series.is_in(voids_lasid_series) & ~total_lasid_series.is_in(history_lasid_series)
        )

    match_lasid_series = pl.concat([found_in_history_series, found_in_voids_series])

    col_to_keep_final_list = ['district_code_name', 'site_name', 'site_code', 'LastName', 'FirstName', 'LASID','DOBDay']

    match_students_df = student_roster_3.filter(
        student_roster_3['LASID'].is_in(match_lasid_series)
        ).select(col_to_keep_final_list)

    no_match_students_df = student_roster_3.filter(
        ~(total_lasid_series.is_in(voids_lasid_series) | total_lasid_series.is_in(history_lasid_series))
        ).select(col_to_keep_final_list)

    student_roster_4 = student_roster_3.with_columns(
        pl.when(pl.col('LASID').is_in(match_lasid_series))
        .then(pl.lit('Y'))
        .otherwise(pl.lit('N')).alias('if_matched'))

    history_test_4_match = history_test_2.filter(history_test_2['LASID_new'].cast(pl.Float64).is_in(match_lasid_series))


    history_test_4_match_2 = history_test_4_match.with_columns(
        pl.when(pl.col('Source').str.to_lowercase().str.contains('la'))
        .then(1)
        .when(pl.col('Source').str.to_lowercase().str.contains('sea'))
        .then(0)
        .otherwise(None)
        .alias('priority')
    )


    # List of columns based on which you want to sort the DataFrame
    sort_columns_ACT_score = config['sort_columns_ACT_score']

    history_test_4_match_2 = history_test_4_match_2.with_columns(pl.col('Score_Composite').fill_nan(0).alias('Score_Composite'))

    history_test_4_match_sorted = history_test_4_match_2.sort(sort_columns_ACT_score, descending=True)

    history_test_4_match_dedup = history_test_4_match_sorted.unique(subset='LASID_new_2', keep='first')

    LASID_score_df = history_test_4_match_dedup.select(['LASID_new', 'Score_Composite']).rename(
            {'LASID_new': 'LASID', 'Score_Composite': 'score_composite'}
        ).with_columns(
            pl.col('LASID').cast(pl.Float64).cast(pl.Int64).alias('LASID')
        )

    match_students_with_score_df = match_students_df.join(
        LASID_score_df, on='LASID', how='left').with_columns([
            pl.col('score_composite').fill_null(0).cast(pl.Float64).fill_nan(0).cast(pl.Int64).cast(pl.Utf8).alias('score_composite'),
            pl.when(pl.col('LASID').is_in(found_in_voids_series)).then(pl.lit('Y')).alias('district_pay')
    ])

    column_mapping = {'district_code_name': 'split', 'site_name': 'School_Name', 'site_code': 'Site_Code', 'LastName': 'Last_Name', 'FirstName': 'First_Name', 'DOBDay': 'Birth_Date'}

    match_students_final_df = match_students_with_score_df.rename(column_mapping).with_columns(
        pl.col('LASID').cast(pl.Utf8).alias('LASID')
    )

    no_match_students_final_df = no_match_students_df.rename(column_mapping).with_columns(
        pl.col('LASID').cast(pl.Utf8).alias('LASID')
    )

    export_match_nomatch(config, match_students_final_df, no_match_students_final_df)

    return nov_lap_11, student_roster_4



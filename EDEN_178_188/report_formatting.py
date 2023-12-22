import polars as pl


def generate_total_indcatator_add_explanation(df, subgroup_list):
    # Create an expression that checks if all columns in subgroup_list are null
    all_null_expr = pl.col(subgroup_list[0]).is_null()
    for col_name in subgroup_list[1:]:
        all_null_expr = all_null_expr & pl.col(col_name).is_null()

    return df.with_columns([
        pl.when(all_null_expr).then(pl.lit('Y')).otherwise(pl.lit('N')).alias("total_indicator"),
        pl.lit('').alias("explanation")
    ])


def add_header_to_report(df, aggregation_type, report_type, report_column_number, report_period):
    # Assign file_type_seg and short_seg based on aggregation_type
    if aggregation_type == 'school':
        file_type_seg, short_seg = 'SCHOOL', 'SCH'
    elif aggregation_type == 'district':
        file_type_seg, short_seg = 'LEA', 'LEA'
    elif aggregation_type == 'state':
        file_type_seg, short_seg = 'SEA', 'SEA'
    else:
        raise ValueError('Invalid aggregation type')
    # Assgin  file_type_seg_2, filename_seg, identifier_seg based on report_type
    if report_type == 178:
        file_type_seg_2, filename_seg, identifier_seg = 'STUDENT PERFORMANCE READING LANGUAGE ARTS', 'STUPERRLA', '178'
    elif report_type == 188:
        file_type_seg_2, filename_seg, identifier_seg = 'RLA ASSESSMENT PARTICIPATION', 'STUPARTRL', '188'
    else:
        raise ValueError('Invalid report type')

    # Header information
    file_type = f'{file_type_seg} {file_type_seg_2}'
    total_records = str(df.height)
    file_name = f'LA{short_seg}{filename_seg}0000001.csv'
    file_identifier = f'N{identifier_seg} {short_seg} {report_period}'
    file_reporting_period = f'{report_period}'
    filler = ''

    header = [file_type, total_records, file_name, file_identifier, file_reporting_period, filler] + ['' for _ in range(report_column_number-6)]

    # Create a DataFrame for the header row
    header_df = pl.DataFrame([header], schema=df.columns)

    # Concatenate the header DataFrame with school_level_summary_178_2
    df_2 = pl.concat([header_df, df.cast(pl.Utf8)])
    return df_2


def dedup_add_indicator_header(df, config, aggregation_type, report_type):

    # 2. Flatten the list and remove duplicates
    subgroup_list = list(set().union(*config['subgroups']))
    # 3. Check for nulls in subgroup_list and assign 'total_indicator'
    df2 = generate_total_indcatator_add_explanation(df, subgroup_list)
    if report_type == 178:
        col_name = f'formatted_col_to_keep_{report_type}'
    elif report_type == 188:
        col_name = f'formatted_col_to_keep_{report_type}'
    else:
        raise ValueError('Invalid report type')
    # Select and reorder columns
    df2 = df2.select(config[col_name])
    return add_header_to_report(df2, aggregation_type, report_type, len(config[col_name]), config['report_period'])



# Format the report for 178
def format_report_178_school(school_level_summary_178, config):
    
    # 178 school level
    # 1. Add state and district info
    school_level_summary_178_2 = school_level_summary_178.with_columns([
        pl.arange(1, school_level_summary_178.height + 1).alias("file_record_number"),
        pl.lit("22").alias("state_code"),
        pl.lit("01").alias("state_agency_number"),
        school_level_summary_178['EDEN_sitecode'].str.slice(0, 3).alias("lea_identifier"),
        school_level_summary_178['EDEN_sitecode'].alias("school_identifier"),
        pl.when(school_level_summary_178['transformed_grade'] == config['HS_GRADE_ONE_VALUE'])
        .then(pl.lit('STUDPERFRLAHS'))
        .otherwise(pl.lit('STUDPERFRLALG'))
        .alias("table_name"),
        pl.lit('').alias("filler")
    ])

    return dedup_add_indicator_header(school_level_summary_178_2, config, 'school', 178)


def format_report_178_district(district_level_summary_178, config):
    
    # 178 district level
    # 1. Add state and district info
    district_level_summary_178_2 = district_level_summary_178.with_columns([
        pl.arange(1, district_level_summary_178.height + 1).alias("file_record_number"),
        pl.lit("22").alias("state_code"),
        pl.lit("01").alias("state_agency_number"),
        district_level_summary_178['EDEN_leacode'].alias("lea_identifier"),
        pl.lit("").alias("school_identifier"),
        pl.when(district_level_summary_178['transformed_grade'] == config['HS_GRADE_ONE_VALUE'])
        .then(pl.lit('STUDPERFRLAHS'))
        .otherwise(pl.lit('STUDPERFRLALG'))
        .alias("table_name"),
        pl.lit('').alias("filler")
    ])

    return dedup_add_indicator_header(district_level_summary_178_2, config, 'district', 178)


def format_report_178_state(all_combination_178, config):
    
    # 178 state level
    # 1. Add state and district info
    state_level_summary_178_2 = all_combination_178.with_columns([
        pl.arange(1, all_combination_178.height + 1).alias("file_record_number"),
        pl.lit("22").alias("state_code"),
        pl.lit("01").alias("state_agency_number"),
        pl.lit("").alias("lea_identifier"),
        pl.lit("").alias("school_identifier"),
        pl.when(all_combination_178['transformed_grade'] == config['HS_GRADE_ONE_VALUE'])
        .then(pl.lit('STUDPERFRLAHS'))
        .otherwise(pl.lit('STUDPERFRLALG'))
        .alias("table_name"),
        pl.lit('').alias("filler")
    ])

    return dedup_add_indicator_header(state_level_summary_178_2, config, 'state', 178)


def format_report_188_school(school_level_summary_188, config):
    
    # 188 school level
    # 1. Add state and district info
    school_level_summary_188_2 = school_level_summary_188.with_columns([
        pl.arange(1, school_level_summary_188.height + 1).alias("file_record_number"),
        pl.lit("22").alias("state_code"),
        pl.lit("01").alias("state_agency_number"),
        school_level_summary_188['EDEN_sitecode'].str.slice(0, 3).alias("lea_identifier"),
        school_level_summary_188['EDEN_sitecode'].alias("school_identifier"),
        pl.when(school_level_summary_188['transformed_grade'] == config['HS_GRADE_ONE_VALUE'])
        .then(pl.lit('STUDPERFRLAHS'))
        .otherwise(pl.lit('STUDPERFRLALG'))
        .alias("table_name"),
        pl.lit('').alias("filler"),
        pl.lit('').alias("filler2"),
        pl.lit('').alias("filler3"),
        
    ])

    return dedup_add_indicator_header(school_level_summary_188_2, config, 'school', 188)


def format_report_188_district(district_level_summary_188, config):
    
    # 188 district level
    # 1. Add state and district info
    district_level_summary_188_2 = district_level_summary_188.with_columns([
        pl.arange(1, district_level_summary_188.height + 1).alias("file_record_number"),
        pl.lit("22").alias("state_code"),
        pl.lit("01").alias("state_agency_number"),
        district_level_summary_188['EDEN_leacode'].alias("lea_identifier"),
        pl.lit("").alias("school_identifier"),
        pl.when(district_level_summary_188['transformed_grade'] == config['HS_GRADE_ONE_VALUE'])
        .then(pl.lit('STUDPERFRLAHS'))
        .otherwise(pl.lit('STUDPERFRLALG'))
        .alias("table_name"),
        pl.lit('').alias("filler"),
        pl.lit('').alias("filler2"),
        pl.lit('').alias("filler3")
    ])

    return dedup_add_indicator_header(district_level_summary_188_2, config, 'district', 188)


def format_report_188_state(all_combination_188, config):
    
    # 188 state level
    # 1. Add state and district info
    state_level_summary_188_2 = all_combination_188.with_columns([
        pl.arange(1, all_combination_188.height + 1).alias("file_record_number"),
        pl.lit("22").alias("state_code"),
        pl.lit("01").alias("state_agency_number"),
        pl.lit("").alias("lea_identifier"),
        pl.lit("").alias("school_identifier"),
        pl.when(all_combination_188['transformed_grade'] == config['HS_GRADE_ONE_VALUE'])
        .then(pl.lit('STUDPERFRLAHS'))
        .otherwise(pl.lit('STUDPERFRLALG'))
        .alias("table_name"),
        pl.lit('').alias("filler"),
        pl.lit('').alias("filler2"),
        pl.lit('').alias("filler3")
    ])

    return dedup_add_indicator_header(state_level_summary_188_2, config, 'state', 188)


def formatting_reports(intermediate_list, config):
    (school_summary_178, 
     district_summary_178,
     state_summary_178,
     school_summary_188, 
     district_summary_188,
     state_summary_188
     ) = intermediate_list
    

    formatted_school_178 = format_report_178_school(school_summary_178, config)
    formatted_district_178 = format_report_178_district(district_summary_178, config)
    formatted_state_178 = format_report_178_state(state_summary_178, config)
    formatted_school_188 = format_report_188_school(school_summary_188, config)
    formatted_district_188 = format_report_188_district(district_summary_188, config)
    formatted_state_188 = format_report_188_state(state_summary_188, config)

    return (formatted_school_178, 
            formatted_district_178,
            formatted_state_178,
            formatted_school_188, 
            formatted_district_188,
            formatted_state_188
            )
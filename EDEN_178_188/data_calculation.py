from itertools import product
import polars as pl


def generate_group_by(df, basic_group, subgroup, all_subgroups):
    # Adding all subgroup columns with None as default
    new_columns = [pl.lit("").alias(s) for s in all_subgroups if s not in subgroup]
    if new_columns:
        df = df.with_columns(new_columns)

    # Filter out rows where any of the grouping columns have null values
    group_columns = basic_group + all_subgroups
    for col in group_columns:
        df = df.filter(pl.col(col).is_not_null())

    return df.group_by(group_columns).agg(pl.count("lasid").alias("Counts"))


# Function to create a cartesian product of unique values from multiple columns
def cartesian_product(df, columns, all_subgroups_total):
    # Extract unique values for each column and create a list of lists
    unique_values = [df[col].unique().to_list() for col in columns]
    not_used_dict = {}
    # Generate cartesian product of these lists
    combinations = list(product(*unique_values))

    # Convert combinations to a DataFrame
    # Create a dictionary where each key is a column name and the value is a list of column values
    data_dict = {columns[i]: [combo[i] for combo in combinations] for i in range(len(columns))}
  
    for col in all_subgroups_total:
        if col not in columns:
            not_used_dict.update({col: ["" for _ in combinations]})

    data_dict.update(not_used_dict)
   
    return pl.DataFrame(dict(sorted(data_dict.items())))



# Function to count occurrences for each combination
def count_combinations(df, grouping_columns, all_subgroups_total):
    # Create a cartesian product for the unique values in the grouping columns
    all_combinations = cartesian_product(df, grouping_columns, all_subgroups_total)

    # Count occurrences of each combination in the data
    count_df = (
        df.group_by(grouping_columns)
        .agg(pl.count('lasid').alias('Counts'))
    )

    # Merge to include combinations with zero count
    merged_df = all_combinations.join(count_df, on=grouping_columns, how='left')

    # Fill NaN values with 0 only for the 'count' column and update the DataFrame
    return merged_df.with_columns([
        pl.col('Counts')
            .fill_nan(0)
            .fill_null(0)
    ])


def student_level_aggregation(**kwargs):

    student_roster_178_df = kwargs.get('roster_178_df')
    student_roster_188_df = kwargs.get('roster_188_df')
    basic_group = kwargs.get('basic_group')
    basic_group_188 = kwargs.get('basic_group_188')
    subgroups = kwargs.get('subgroups')
    HS_GRADE_ONE_VALUE = kwargs.get('HS_GRADE_ONE_VALUE')

    # aggregation by subgroups
    # Flatten the list of subgroups to get all possible subgroup columns
    all_subgroups = list(set([item for sublist in subgroups for item in sublist]))

    school_level_summary_178 = pl.concat([
        generate_group_by(student_roster_178_df, ['EDEN_sitecode'] + basic_group, group, all_subgroups)
        for group in subgroups
    ])

    # Similarly for student_roster_188
    school_level_summary_188 = pl.concat([
        generate_group_by(student_roster_188_df, ['EDEN_sitecode'] + basic_group_188, group, all_subgroups)
        for group in subgroups
    ])

    district_level_summary_178 = pl.concat([
        generate_group_by(student_roster_178_df, ['EDEN_leacode'] + basic_group, group, all_subgroups)
        for group in subgroups
    ])

    district_level_summary_188 = pl.concat([
        generate_group_by(student_roster_188_df, ['EDEN_leacode'] + basic_group_188, group, all_subgroups)
        for group in subgroups
    ])

    # For state level, all the zero counts are needed, so we need to generate all possible combinations of subgroups, which is as know as cartesian product
    # For 178
    all_subgroups_total_178 = list(set([item for sublist in subgroups for item in sublist] + basic_group))
    # Iterate over subgroups and perform the aggregation
    results = []
    for subgroup in subgroups:
        result_df = count_combinations(student_roster_178_df, basic_group + subgroup, all_subgroups_total_178)
        results.append(result_df)
    # Concatenate all results into a single DataFrame
    final_result = pl.concat(results)

    mask_iap_keep = final_result['assessment_administered'].is_in(['IADAPLASMTWOACC', 'IADAPLASMTWACC']) & final_result['transformed_grade'].is_in(['06', '07', '08'])

    # filter out combination that is not logically correct
    mask_keep_k8 = (final_result['transformed_grade'].is_in([str(i).zfill(2) for i in range(3, 9)]) & \
        (final_result['assessment_administered'].is_in(['REGASSWOACC', 'REGASSWACC', 'ALTASSALTACH']) ) ) | mask_iap_keep

    mask_keep_hs = final_result['transformed_grade'].is_in([HS_GRADE_ONE_VALUE]) & \
        (final_result['assessment_administered'].is_in(['HSREGASMTIWOACC', 'HSREGASMTIWACC', 'ALTASSALTACH']) )

    all_combination_178 = final_result.filter(mask_keep_k8 | mask_keep_hs).drop_nulls()

    # For 188
    all_subgroups_total_188 = list(set([item for sublist in subgroups for item in sublist] + basic_group_188))

    # Iterate over subgroups and perform the aggregation
    results_188 = []
    for subgroup in subgroups:
        result_188_df = count_combinations(student_roster_188_df, basic_group_188 + subgroup, all_subgroups_total_188)
        results_188.append(result_188_df)
    # Concatenate all results into a single DataFrame
    final_result_188 = pl.concat(results_188)

    mask_iap_keep_188 = final_result_188['participation_status'].is_in(['PIADAPLASMWOACC', 'PIADAPLASMWACC']) & final_result_188['transformed_grade'].is_in(['06', '07', '08'])

    # filter out combination that is not logically correct
    mask_keep_k8_188 = (final_result_188['transformed_grade'].is_in([str(i).zfill(2) for i in range(3, 9)]) & \
        (final_result_188['participation_status'].is_in(['REGPARTWOACC', 'REGPARTWACC', 'ALTPARTALTACH', 'NPART', 'PARTELP', 'MEDEXEMPT']) ) ) | mask_iap_keep_188

    mask_keep_hs_188 = final_result_188['transformed_grade'].is_in([HS_GRADE_ONE_VALUE]) & \
        (final_result_188['participation_status'].is_in(['PHSRGASMIWOACC', 'PHSRGASMIWACC', 'ALTPARTALTACH', 'PARTELP', 'NPART', 'MEDEXEMPT']) )

    all_combination_188 = final_result_188.filter(mask_keep_k8_188 | mask_keep_hs_188).drop_nulls()

    return [school_level_summary_178, district_level_summary_178, all_combination_178, school_level_summary_188, district_level_summary_188, all_combination_188]


def aggregated_data_calculation(student_roster_178_df, student_roster_188_df, config):
    intermediate_list = student_level_aggregation(
        roster_178_df=student_roster_178_df, 
        roster_188_df=student_roster_188_df,
        basic_group=config['basic_group'],
        basic_group_188=config['basic_group_188'],
        subgroups=config['subgroups'],
        HS_GRADE_ONE_VALUE=config['HS_GRADE_ONE_VALUE']
    )

    return intermediate_list
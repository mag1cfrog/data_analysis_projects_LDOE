import duckdb


def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()


def duckdb_student_level_data_cleaning(conn, SQL_QUERY_LOCATION): 
    # Read the SQL query from the file
    query1 = read_sql_file(SQL_QUERY_LOCATION)
    conn.execute(query1)


def duckdb_to_polars(conn):
    # transform duckDB database to polars dataframe, since polars is more convenient for complex functions
    query4 = """
    -- sql
    SELECT *
    FROM student_roster_178;
    """
    query5 = """
    -- sql
    SELECT *
    FROM student_roster_188;
    """
    student_roster_178_df = conn.execute(query4).pl()
    student_roster_188_df = conn.execute(query5).pl()
    return student_roster_178_df, student_roster_188_df


def duckdb_data_extract_clean(config):
    duckdb_directory = config['DUCKDB_DIRECTORY']
    sql_query_location = config['SQL_QUERY_LOCATION']
    conn = duckdb.connect(database=duckdb_directory)
    duckdb_student_level_data_cleaning(conn, sql_query_location)
    return duckdb_to_polars(conn)




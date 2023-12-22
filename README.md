# Data Analysis Projects for LDOE

## Overview
This repository hosts a collection of data analysis projects developed for the Louisiana Department of Education (LDOE). These projects showcase skills in data processing, analysis, and handling large datasets with various technologies, particularly focusing on the use of Polars and DuckDB.

## Projects

### ACT Match/No Match
- **Purpose**: This project involves matching and identifying non-matching records between ACT data and other educational datasets, showcasing the use of Polars for efficient data manipulation.
- **Technologies Used**: 
  - Python for data processing and analysis.
  - Polars for efficient data manipulation and analysis.
  - JSON for configuration management.
- **Key Features**:
  - Utilization of Polars for fast and memory-efficient data processing.
  - Efficient matching algorithm to compare and contrast large datasets.
  - Customizable configuration for different data sources.
  - Comprehensive data preprocessing and cleaning steps.
- **Files**:
  - [Main Script](https://github.com/mag1cfrog/data_anlysis_projects_LDOE/blob/master/ACT_match_nomatch/main.py)
  - [Configuration](https://github.com/mag1cfrog/data_anlysis_projects_LDOE/blob/master/ACT_match_nomatch/config_match_no_match.json)
  - [Utility Functions](https://github.com/mag1cfrog/data_anlysis_projects_LDOE/blob/master/ACT_match_nomatch/utils.py)
  - [Data Processing Scripts](https://github.com/mag1cfrog/data_anlysis_projects_LDOE/tree/master/ACT_match_nomatch)

### EDEN 178-188
- **Purpose**: This project focuses on cleaning and formatting student-level data for EDEN reporting, demonstrating the use of DuckDB and Polars for efficient data handling and analysis.
- **Technologies Used**: 
  - DuckDB for efficient database operations.
  - Polars for high-performance data manipulation.
  - SQL for data querying and manipulation.
- **Key Features**:
  - Integration of DuckDB for optimized data storage and querying.
  - Use of Polars for fast and memory-efficient data processing.
  - SQL-based data cleaning and transformation.
  - Generation of formatted reports for EDEN submission.
- **Files**:
  - [Main Script](https://github.com/mag1cfrog/data_anlysis_projects_LDOE/blob/master/EDEN_178_188/main.py)
  - [Data Calculation](https://github.com/mag1cfrog/data_anlysis_projects_LDOE/blob/master/EDEN_178_188/data_calculation.py)
  - [DuckDB Operations](https://github.com/mag1cfrog/data_anlysis_projects_LDOE/blob/master/EDEN_178_188/duckdb_operation.py)
  - [SQL Queries](https://github.com/mag1cfrog/data_anlysis_projects_LDOE/blob/master/EDEN_178_188/student_level_data_cleaning.sql)

### LEAP (Louisiana Educational Assessment Program)
- **Purpose**: Scripts for extracting and processing data from PDF files.
- **Technologies Used**: 
  - Pandas and PySpark for data processing.
  - Python for scripting.
- **Features**:
  - Efficient data extraction from PDFs.
  - Scalable data processing suitable for large datasets.
- **Files**:
  - [Data Extraction Tools](https://github.com/mag1cfrog/data_anlysis_projects_LDOE/tree/master/LEAP/Extract_PDF_Data_spark/PDFDataExtractionTools)
  - [Main Script](https://github.com/mag1cfrog/data_anlysis_projects_LDOE/blob/master/LEAP/Extract_PDF_Data_spark/main.py)

## Usage
- Python 3.x is required.
- Additional libraries: Pandas, PySpark, Polars, DuckDB (as per project requirements).

## Contribution
Contributions are welcome! Please fork the repository, create a new branch for your features, and submit a pull request.

## License
This project is licensed under the GPL-3.0 License. See the [LICENSE](https://github.com/mag1cfrog/data_anlysis_projects_LDOE/blob/master/LICENSE) file for details.

## Contact
For inquiries, collaborations, or feedback, please reach out to:
- Email: [harrywong2017@gmail.com](mailto:harrywong2017@gmail.com)
- LinkedIn: [Hanbo Wang](https://www.linkedin.com/in/hanbo-wang-mag1cfrog/)
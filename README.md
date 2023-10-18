# LDOE Projects

## Overview
This repository contains Python scripts developed for the Louisiana Department of Education (LDOE) to extract and process data from PDF files. The scripts demonstrate proficiency in data processing, parallel computing, and big data technologies.

## Projects

### LEAP (Louisiana Educational Assessment Program)

#### Extract_PDF_Data.py
- **Purpose**: This script is designed to extract data from PDF files using a parallelized approach.
- **Technologies Used**: 
  - **Pandas**: For data manipulation and analysis.
  - **ProcessPoolExecutor**: To achieve parallel processing, enhancing the speed and efficiency of data extraction.
- **Features**:
  - Efficient data extraction from multiple PDFs simultaneously.
  - Data cleaning and preprocessing using Pandas.
  - Exporting extracted data to desired formats such as CSV or Excel.

#### Extract_PDF_Data_spark.py
- **Purpose**: An enhanced version of the original script, optimized for larger datasets and distributed computing environments.
- **Technologies Used**: 
  - **PySpark**: A big data framework that offers distributed data processing capabilities.
- **Features**:
  - Scalable data extraction suitable for large datasets.
  - Utilizes the power of distributed computing to process data faster and more efficiently.
  - Offers flexibility in handling various data formats and sources.

## Usage

### Prerequisites
- Python 3.x
- Libraries: Pandas, PySpark
- Java (for PySpark)

### Running the Scripts
1. Clone the repository: `git clone https://github.com/mag1cfrog/LDOE_Projects.git`
2. Navigate to the LEAP directory: `cd LDOE_Projects/LEAP`
3. For the original script: `python Extract_PDF_Data.py [path_to_PDFs]`
4. For the PySpark version: `spark-submit Extract_PDF_Data_spark.py [path_to_PDFs]`

Replace `[path_to_PDFs]` with the directory containing your PDF files.

## Contribution
Contributions are welcome! If you have improvements or bug fixes, please:
1. Fork the repository.
2. Create a new branch for your features.
3. Submit a pull request.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact
For further inquiries, collaborations, or feedback, please reach out to:
- Email: [harrywong2017@gmail.com](mailto:harrywong2017@gmail.com)
- LinkedIn: [Hanbo Wang](https://www.linkedin.com/in/hanbo-wang-mag1cfrog/)
